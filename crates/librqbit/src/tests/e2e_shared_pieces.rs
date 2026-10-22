use std::{net::Ipv4Addr, num::NonZeroU32, sync::Arc, time::Duration};

use anyhow::Context;
use librqbit_core::lengths::{Lengths, ValidPieceIndex};
use tempfile::TempDir;
use tokio::time::timeout;

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, PieceSelector, Session, SessionOptions,
    create_torrent,
    limits::LimitsConfig,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    tests::test_util::{
        TestPeerMetadata, create_default_random_dir_with_torrents, setup_test_logging,
    },
    torrent_state::live::peer::stats::snapshot::PeerStatsFilter,
};

const PIECE_LENGTH: u32 = 256 * 1024;
/// Slow enough that one seeder alone takes seconds over the piece.
const SEEDER_UPLOAD_BPS: u32 = 128 * 1024;

/// Requests only piece 0 and offers it for sharing, as a reader's current
/// piece is.
struct UrgentFirstPiece(ValidPieceIndex);

impl PieceSelector for UrgentFirstPiece {
    fn priority_pieces(&self, f: &mut dyn FnMut(ValidPieceIndex) -> bool) {
        f(self.0);
    }

    fn download_other_pieces(&self) -> bool {
        false
    }

    fn shared_pieces(&self, f: &mut dyn FnMut(ValidPieceIndex) -> bool) {
        f(self.0);
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

/// An urgent piece offered for sharing arrives from several seeders, not
/// only from the peer that reserved it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shared_piece_arrives_from_several_peers() -> anyhow::Result<()> {
    setup_test_logging();
    let files = create_default_random_dir_with_torrents(1, 1024 * 1024, Some("test_e2e_shared"));
    let torrent = create_torrent(
        files.path(),
        CreateTorrentOptions {
            name: None,
            piece_length: Some(PIECE_LENGTH),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?;
    let bytes = torrent.as_bytes()?;

    let mut seeders = Vec::new();
    let mut peers = Vec::new();
    for _ in 0..3 {
        let seeder = Session::new_with_opts(files.path().into(), options(true)).await?;
        timeout(
            Duration::from_secs(10),
            seeder
                .add_torrent(
                    AddTorrent::from_bytes(bytes.clone()),
                    Some(AddTorrentOptions {
                        output_folder: Some(files.path().to_str().unwrap().to_owned()),
                        overwrite: true,
                        ratelimits: LimitsConfig {
                            upload_bps: NonZeroU32::new(SEEDER_UPLOAD_BPS),
                            download_bps: None,
                        },
                        ..Default::default()
                    }),
                )
                .await?
                .into_handle()
                .context("the seeded torrent")?
                .wait_until_completed(),
        )
        .await??;
        peers.push(seeder.listen_addr().context("a seeder's address")?);
        seeders.push(seeder);
    }

    let first_piece = Lengths::new(1024 * 1024, PIECE_LENGTH)?
        .validate_piece_index(0)
        .context("piece 0")?;
    let client_dir = TempDir::with_prefix("test_e2e_shared_client")?;
    let client = Session::new_with_opts(client_dir.path().into(), options(false)).await?;
    let handle = client
        .add_torrent(
            AddTorrent::from_bytes(bytes),
            Some(AddTorrentOptions {
                initial_peers: Some(peers),
                overwrite: true,
                piece_selector: Some(Arc::new(UrgentFirstPiece(first_piece))),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("the client torrent")?;

    timeout(Duration::from_secs(20), async {
        loop {
            let done = handle
                .live()
                .is_some_and(|live| live.stats_snapshot().downloaded_and_checked_pieces >= 1);
            if done {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("piece 0 was not downloaded")?;

    let live = handle.live().context("the client torrent is live")?;
    let delivering = live
        .per_peer_stats_snapshot(PeerStatsFilter::default())
        .peers
        .values()
        .filter(|peer| peer.counters.fetched_bytes > 0)
        .count();
    assert!(
        delivering >= 2,
        "{delivering} peer delivered piece 0's chunks; sharing should spread it"
    );
    assert_eq!(
        live.stats_snapshot().downloaded_and_checked_pieces,
        1,
        "only the urgent piece was requested"
    );

    client.stop().await;
    for seeder in seeders {
        seeder.stop().await;
    }
    Ok(())
}
