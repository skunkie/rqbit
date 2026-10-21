use std::{net::Ipv4Addr, sync::Arc, time::Duration};

use anyhow::Context;
use librqbit_core::lengths::ValidPieceIndex;
use tempfile::TempDir;
use tokio::time::{Instant, timeout};

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, ManagedTorrent, PeerConnectionOptions,
    PieceSelector, Session, SessionOptions, create_torrent,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    tests::test_util::{
        TestPeerMetadata, create_default_random_dir_with_torrents, setup_test_logging,
    },
};

/// How long the seeder lets a peer send nothing before dropping it.
const SEEDER_IDLE_TIMEOUT: Duration = Duration::from_secs(1);

/// Selects nothing, as a memory torrent without streams does, so the
/// client's peer connection stays idle without the torrent finishing.
struct NothingSelected;

impl PieceSelector for NothingSelected {
    fn priority_pieces(&self, _f: &mut dyn FnMut(ValidPieceIndex) -> bool) {}

    fn download_other_pieces(&self) -> bool {
        false
    }
}

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

/// A seeder that drops idle peers after [`SEEDER_IDLE_TIMEOUT`], and an idle
/// client torrent connected to it with `keep_alive_interval`.
struct IdlePair {
    seeder: Arc<Session>,
    client: Arc<Session>,
    handle: Arc<ManagedTorrent>,
    _files: TempDir,
    _client_dir: TempDir,
}

impl IdlePair {
    async fn start(keep_alive_interval: Option<Duration>) -> anyhow::Result<Self> {
        setup_test_logging();
        let files = create_default_random_dir_with_torrents(1, 65536, Some("test_e2e_idle"));
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
                        peer_opts: Some(PeerConnectionOptions {
                            read_write_timeout: Some(SEEDER_IDLE_TIMEOUT),
                            ..Default::default()
                        }),
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

        let client_dir = TempDir::with_prefix("test_e2e_idle_client")?;
        let client = Session::new_with_opts(client_dir.path().into(), options(false)).await?;
        let handle = client
            .add_torrent(
                AddTorrent::from_bytes(bytes),
                Some(AddTorrentOptions {
                    initial_peers: Some(vec![peer]),
                    overwrite: true,
                    piece_selector: Some(Arc::new(NothingSelected)),
                    peer_opts: Some(PeerConnectionOptions {
                        keep_alive_interval,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .context("the idle torrent")?;
        let pair = Self {
            seeder,
            client,
            handle,
            _files: files,
            _client_dir: client_dir,
        };
        pair.wait_for_live_peers(1, Duration::from_secs(10))
            .await
            .context("the client never connected")?;
        Ok(pair)
    }

    fn live_peers(&self) -> usize {
        self.handle
            .live()
            .map_or(0, |live| live.stats_snapshot().peer_stats.live as usize)
    }

    async fn wait_for_live_peers(&self, count: usize, within: Duration) -> anyhow::Result<()> {
        timeout(within, async {
            while self.live_peers() != count {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .with_context(|| format!("{} live peers, not {count}", self.live_peers()))
    }

    async fn stop(self) {
        self.client.stop().await;
        self.seeder.stop().await;
    }
}

/// A torrent's keep-alive interval reaches its peer connections, so an idle
/// peer that drops silent connections stays connected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keep_alives_keep_an_idle_peer() -> anyhow::Result<()> {
    let pair = IdlePair::start(Some(SEEDER_IDLE_TIMEOUT / 4)).await?;
    tokio::time::sleep(SEEDER_IDLE_TIMEOUT * 3).await;
    assert_eq!(pair.live_peers(), 1, "the idle peer stayed connected");
    pair.stop().await;
    Ok(())
}

/// reconnect_peers connects a peer that died at once, rather than after
/// its backoff.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconnect_peers_skips_the_backoff() -> anyhow::Result<()> {
    let pair = IdlePair::start(None).await?;
    pair.wait_for_live_peers(0, SEEDER_IDLE_TIMEOUT * 5)
        .await
        .context("the seeder never dropped the silent peer")?;
    let reconnecting = Instant::now();
    pair.handle.reconnect_peers();
    pair.wait_for_live_peers(1, Duration::from_secs(1))
        .await
        .context("the peer did not reconnect promptly")?;
    tracing::info!(elapsed = ?reconnecting.elapsed(), "reconnected");
    pair.stop().await;
    Ok(())
}
