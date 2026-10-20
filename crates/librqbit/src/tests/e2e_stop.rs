use std::{net::Ipv4Addr, time::Duration};

use anyhow::Context;
use tempfile::TempDir;
use tokio::{io::AsyncReadExt, time::timeout};

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, Session, SessionOptions, create_torrent,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    tests::test_util::{
        TestPeerMetadata, create_default_random_dir_with_torrents, setup_test_logging,
    },
};

fn options(listen: bool) -> SessionOptions {
    SessionOptions {
        dht: None,
        persistence: None,
        disable_local_service_discovery: true,
        peer_id: Some(TestPeerMetadata::good().as_peer_id()),
        listen: listen.then(|| ListenerOptions {
            listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
            enable_upnp_port_forwarding: false,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn alive_tasks() -> usize {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
}

/// Session::stop returns once every task the session and its torrents
/// spawned has ended, including those of a torrent that was streaming.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_waits_for_every_session_task() -> anyhow::Result<()> {
    setup_test_logging();
    let before = alive_tasks();
    let files = create_default_random_dir_with_torrents(1, 65536, Some("test_e2e_stop"));
    let torrent = create_torrent(
        files.path(),
        CreateTorrentOptions {
            name: None,
            piece_length: Some(16384),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?;
    let bytes = torrent.as_bytes()?;

    let seeder = Session::new_with_opts(files.path().into(), options(true)).await?;
    timeout(
        Duration::from_secs(10),
        seeder
            .add_torrent(
                AddTorrent::from_bytes(bytes.clone()),
                Some(AddTorrentOptions {
                    output_folder: Some(files.path().to_str().unwrap().to_owned()),
                    overwrite: true,
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .context("the seeded torrent")?
            .wait_until_completed(),
    )
    .await??;
    let peer = seeder.listen_addr().context("the seeder's address")?;

    let client_dir = TempDir::with_prefix("test_e2e_stop_client")?;
    let client = Session::new_with_opts(client_dir.path().into(), options(false)).await?;
    let handle = client
        .add_torrent(
            AddTorrent::from_bytes(bytes),
            Some(AddTorrentOptions {
                initial_peers: Some(vec![peer]),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("the downloading torrent")?;
    let mut stream = handle.clone().stream(0).await?;
    let mut start = [0; 1000];
    timeout(Duration::from_secs(10), stream.read_exact(&mut start)).await??;

    assert!(!seeder.tasks.is_empty() && !client.tasks.is_empty());
    for session in [&client, &seeder] {
        timeout(Duration::from_secs(10), session.stop())
            .await
            .context("stop did not finish")?;
        assert!(session.tasks.is_empty(), "every tracked task ended");
    }
    drop(stream);
    // Ended tasks leave the runtime's count as they are released; a task
    // that was never tracked would keep it up.
    timeout(Duration::from_secs(5), async {
        while alive_tasks() > before {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .with_context(|| format!("{} tasks alive, {before} before", alive_tasks()))?;
    Ok(())
}
