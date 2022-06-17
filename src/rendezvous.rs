use std::time::Duration;

use talk::link::rendezvous::{
    Server as RendezvousServer, ServerSettings as RendezvousServerSettings,
};

use tokio::time;

const ADDRESS: &str = "0.0.0.0:6000";

pub(crate) async fn rendezvous(servers: usize, brokers: usize) {
    let settings = RendezvousServerSettings {
        shard_sizes: vec![servers, brokers],
    };

    let _rendezvous = RendezvousServer::new(ADDRESS, settings).await.unwrap();

    loop {
        time::sleep(Duration::from_secs(1)).await;
    }
}
