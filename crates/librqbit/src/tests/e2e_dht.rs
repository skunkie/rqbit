use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use anyhow::Context;
use bencode::ByteBufOwned;
use tempfile::TempDir;
use tokio::{net::UdpSocket, time::timeout};

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, DhtSessionConfig, Session, SessionOptions,
    create_torrent,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    tests::test_util::{
        TestPeerMetadata, create_default_random_dir_with_torrents, setup_test_logging,
    },
};

fn options(listen: bool, dht: Option<DhtSessionConfig>) -> SessionOptions {
    SessionOptions {
        dht,
        // The DHTs then bind 0.0.0.0 and see loopback senders as 127.0.0.1.
        ipv4_only: true,
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

fn bootstrapping_from(addr: SocketAddr) -> DhtSessionConfig {
    DhtSessionConfig {
        bootstrap_addrs: Some(vec![addr.to_string()]),
        port: None,
        persistence: None,
    }
}

/// Whether the DHT on `port` answers a ping within `wait`.
async fn answers_ping(port: u16, wait: Duration) -> bool {
    let client = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let dht = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    client
        .send_to(
            b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe",
            dht,
        )
        .await
        .unwrap();
    let mut buf = [0u8; 1024];
    timeout(wait, async {
        loop {
            match client.recv_from(&mut buf).await {
                Ok((_, addr)) if addr == dht => return,
                _ => continue,
            }
        }
    })
    .await
    .is_ok()
}

/// set_dht starts a DHT in a session created without one, and stopping it
/// closes it without stopping the session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_dht_starts_and_stops_the_dht() -> anyhow::Result<()> {
    setup_test_logging();
    // Stays open, so the bootstrap query gets neither an answer nor an ICMP error.
    let silent = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let dir = TempDir::with_prefix("test_e2e_dht")?;
    let session = Session::new_with_opts(dir.path().into(), options(false, None)).await?;
    assert!(session.get_dht().is_none());

    session
        .set_dht(Some(bootstrapping_from(silent.local_addr()?)))
        .await?;
    let dht = session.get_dht().context("the started DHT")?;
    let port = dht.listen_addr().port();
    assert!(answers_ping(port, Duration::from_secs(5)).await);

    session.set_dht(None).await?;
    assert!(session.get_dht().is_none());
    assert!(dht.cancellation_token().is_cancelled());
    assert!(!session.cancellation_token().is_cancelled());
    assert!(
        !answers_ping(port, Duration::from_secs(1)).await,
        "a stopped DHT answers nothing"
    );
    session.stop().await;
    Ok(())
}

/// Answers a DHT's queries as a node that knows only itself and no peers,
/// and sends the info hash of every get_peers query it receives.
async fn fake_node(socket: UdpSocket, queried: tokio::sync::mpsc::UnboundedSender<Vec<u8>>) {
    #[derive(serde::Deserialize)]
    struct Query {
        t: ByteBufOwned,
        q: Option<ByteBufOwned>,
        a: Option<Arguments>,
    }
    #[derive(serde::Deserialize)]
    struct Arguments {
        info_hash: Option<ByteBufOwned>,
    }

    let SocketAddr::V4(own) = socket.local_addr().unwrap() else {
        unreachable!("the node binds an IPv4 address");
    };
    // Compact node info: the ID, the IPv4 address, and the port.
    let mut nodes = b"5:nodes26:xxxxxxxxxxxxxxxxxxxx".to_vec();
    nodes.extend_from_slice(&own.ip().octets());
    nodes.extend_from_slice(&own.port().to_be_bytes());

    let mut buf = [0u8; 2048];
    loop {
        let Ok((len, from)) = socket.recv_from(&mut buf).await else {
            return;
        };
        let Ok(query) = bencode::from_bytes::<Query>(&buf[..len]) else {
            continue;
        };
        if query.q.as_ref().map(|q| q.as_ref()) == Some(&b"get_peers"[..])
            && let Some(info_hash) = query.a.and_then(|a| a.info_hash)
        {
            let _ = queried.send(info_hash.as_ref().to_vec());
        }
        let t = query.t.as_ref();
        let mut response = b"d1:rd2:id20:xxxxxxxxxxxxxxxxxxxx".to_vec();
        response.extend_from_slice(&nodes);
        response.extend_from_slice(b"5:token2:tke1:t");
        response.extend_from_slice(format!("{}:", t.len()).as_bytes());
        response.extend_from_slice(t);
        response.extend_from_slice(b"1:y1:re");
        let _ = socket.send_to(&response, from).await;
    }
}

/// A torrent added after set_dht searches for peers through the new DHT,
/// until the DHT stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn torrents_added_after_set_dht_search_through_it() -> anyhow::Result<()> {
    setup_test_logging();
    let node = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let node_addr = node.local_addr()?;
    let (queried, mut queries) = tokio::sync::mpsc::unbounded_channel();
    let node_task = tokio::spawn(fake_node(node, queried));

    let files = create_default_random_dir_with_torrents(1, 16384, Some("test_e2e_dht"));
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
    let info_hash = torrent.info_hash();

    let dir = TempDir::with_prefix("test_e2e_dht_client")?;
    let session = Session::new_with_opts(dir.path().into(), options(false, None)).await?;
    session.set_dht(Some(bootstrapping_from(node_addr))).await?;
    session
        .add_torrent(
            AddTorrent::from_bytes(torrent.as_bytes()?),
            Some(AddTorrentOptions {
                overwrite: true,
                ..Default::default()
            }),
        )
        .await?;
    timeout(Duration::from_secs(20), async {
        while let Some(queried) = queries.recv().await {
            if queried[..] == info_hash.0[..] {
                return;
            }
        }
    })
    .await
    .context("the torrent never searched the DHT")?;

    // Stopping the DHT ends the running torrent's search at once.
    session.set_dht(None).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    while queries.try_recv().is_ok() {}
    assert!(
        timeout(Duration::from_secs(3), queries.recv())
            .await
            .is_err(),
        "no query after the DHT stopped"
    );

    session.stop().await;
    node_task.abort();
    Ok(())
}
