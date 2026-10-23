//! The embedder's initial bitfield in place of the full check.

use std::path::Path;

use anyhow::Context;
use bitvec::{bitvec, order::Msb0};
use tempfile::TempDir;

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, Session, SessionOptions, create_torrent,
    spawn_utils::BlockingSpawner,
    tests::test_util::{create_new_file_with_random_content, setup_test_logging},
};

const PIECE_LENGTH: u32 = 16 * 1024;
/// More pieces than the sampled validation checks, so a skipped full check
/// shows in the result. Not a multiple of eight, so the bitfield has spare
/// bits.
const PIECES: usize = 259;

struct Fixture {
    content: TempDir,
    torrent: bytes::Bytes,
}

async fn fixture() -> anyhow::Result<Fixture> {
    setup_test_logging();
    let content = TempDir::with_prefix("test_initial_pieces")?;
    create_new_file_with_random_content(
        &content.path().join("0.data"),
        PIECES * PIECE_LENGTH as usize,
    );
    let torrent = create_torrent(
        content.path(),
        CreateTorrentOptions {
            name: None,
            piece_length: Some(PIECE_LENGTH),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?
    .as_bytes()?;
    Ok(Fixture { content, torrent })
}

/// The bytes the first check finds when the torrent's files are in `folder`
/// and `initial_pieces` is given.
async fn checked_bytes(
    fixture: &Fixture,
    folder: &Path,
    initial_pieces: Option<Vec<u8>>,
) -> anyhow::Result<u64> {
    let session = Session::new_with_opts(
        folder.into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_local_service_discovery: true,
            listen: None,
            ..Default::default()
        },
    )
    .await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(fixture.torrent.clone()),
            Some(AddTorrentOptions {
                output_folder: Some(folder.to_str().context("UTF-8 paths")?.to_owned()),
                overwrite: true,
                paused: true,
                initial_pieces,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("the torrent")?;
    handle.wait_until_initialized().await?;
    let progress = handle.stats().progress_bytes;
    let have: u32 = handle
        .have_pieces()?
        .iter()
        .map(|byte| byte.count_ones())
        .sum();
    assert_eq!(
        u64::from(have) * PIECE,
        progress,
        "the verified pieces account for the progress"
    );
    session.stop().await;
    Ok(progress)
}

/// A bitfield claiming the first `claimed` pieces, with `spare` setting the
/// bits past the last piece.
fn claiming(claimed: usize, spare: bool) -> Vec<u8> {
    let mut bits = bitvec![u8, Msb0; 0; PIECES.div_ceil(8) * 8];
    bits[..claimed].fill(true);
    if spare {
        bits[PIECES..].fill(true);
    }
    bits.into_vec()
}

const PIECE: u64 = PIECE_LENGTH as u64;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_valid_initial_bitfield_replaces_the_full_check() -> anyhow::Result<()> {
    let fixture = fixture().await?;
    let folder = fixture.content.path();
    assert_eq!(
        checked_bytes(&fixture, folder, None).await?,
        PIECES as u64 * PIECE,
        "the full check finds every piece"
    );
    assert_eq!(
        checked_bytes(&fixture, folder, Some(claiming(100, false))).await?,
        100 * PIECE,
        "the claimed pieces, although the files hold every piece"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bitfield_claiming_missing_pieces_falls_back_to_the_full_check() -> anyhow::Result<()> {
    let fixture = fixture().await?;
    let empty = TempDir::with_prefix("test_initial_pieces_empty")?;
    assert_eq!(
        checked_bytes(&fixture, empty.path(), Some(claiming(PIECES, false))).await?,
        0,
        "no file holds the claimed pieces"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_malformed_bitfield_falls_back_to_the_full_check() -> anyhow::Result<()> {
    let fixture = fixture().await?;
    let folder = fixture.content.path();
    let mut short = claiming(100, false);
    short.pop();
    assert_eq!(
        checked_bytes(&fixture, folder, Some(short)).await?,
        PIECES as u64 * PIECE,
        "a bitfield of the wrong length"
    );
    assert_eq!(
        checked_bytes(&fixture, folder, Some(claiming(100, true))).await?,
        PIECES as u64 * PIECE,
        "a bitfield with bits past the last piece"
    );
    Ok(())
}
