//! Incoming connections whose handshake check fails do not hold up the
//! others.

use std::{net::Ipv4Addr, time::Duration};

use anyhow::Context;
use peer_binary_protocol::Handshake;
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, Session, SessionOptions, create_torrent,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    tests::test_util::{TestPeerMetadata, create_new_file_with_random_content, setup_test_logging},
};

/// A connection that closes before its handshake fails its check. The
/// listener must still hand over a connection whose check completes after
/// that failure, without waiting for yet another connection to arrive.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_check_does_not_stall_a_pending_one() -> anyhow::Result<()> {
    setup_test_logging();
    let content = TempDir::with_prefix("test_incoming_checks")?;
    create_new_file_with_random_content(&content.path().join("0.data"), 64 * 1024);
    let torrent = create_torrent(
        content.path(),
        CreateTorrentOptions {
            name: None,
            piece_length: Some(16 * 1024),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?
    .as_bytes()?;
    let session = Session::new_with_opts(
        content.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_local_service_discovery: true,
            listen: Some(ListenerOptions {
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent),
            Some(AddTorrentOptions {
                output_folder: Some(content.path().to_str().context("UTF-8 paths")?.to_owned()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("the torrent")?;
    timeout(Duration::from_secs(10), handle.wait_until_completed()).await??;
    let addr = session.listen_addr().context("a listening session")?;

    // The pending connection's check waits for its handshake while the
    // other connection's check fails.
    let mut pending = TcpStream::connect(addr).await?;
    let failing = TcpStream::connect(addr).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(failing);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut buf = [0u8; 68];
    let len = Handshake::new(handle.info_hash(), TestPeerMetadata::good().as_peer_id())
        .serialize_unchecked_len(&mut buf);
    pending.write_all(&buf[..len]).await?;
    let mut reply = [0u8; 68];
    timeout(Duration::from_secs(5), pending.read_exact(&mut reply))
        .await
        .context("the listener never handed over the pending connection")??;
    let (reply, _) = Handshake::deserialize(&reply)?;
    assert_eq!(reply.info_hash, handle.info_hash());

    session.stop().await;
    Ok(())
}
