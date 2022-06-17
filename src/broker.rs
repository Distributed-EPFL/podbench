use pod::{CompressedBatch, LoadBroker, Membership};

use std::{
    time::Duration,
    {fs, sync::Arc},
};

use talk::{
    crypto::KeyChain,
    link::rendezvous::{Client as RendezvousClient, Connector as RendezvousConnector},
    net::SessionConnector,
};

use tokio::{
    sync::{Barrier, Semaphore},
    time,
};

pub(crate) async fn broker(
    servers: usize,
    rendezvous: String,
    broker_index: usize,
    workers: usize,
    batches: usize,
) {
    println!("Running as broker:");
    println!("  Expected servers: {}", servers);
    println!("  Rendezvous IP: {}", rendezvous);
    println!("  Broker index: {}", broker_index);
    println!("  Workers per broker: {}", workers);
    println!("  Batches per worker: {}", batches);
    println!();

    println!("Spawning workers..");

    let barrier = Arc::new(Barrier::new(workers));
    let semaphore = Arc::new(Semaphore::new(0));

    for worker_index in 0..workers {
        let rendezvous = rendezvous.clone();

        let barrier = barrier.clone();
        let semaphore = semaphore.clone();

        tokio::spawn(async move {
            run(
                servers,
                rendezvous,
                broker_index,
                workers,
                batches,
                barrier,
                semaphore,
                worker_index,
            )
            .await;
        });
    }

    loop {
        time::sleep(Duration::from_secs(1)).await;
    }
}

async fn run(
    servers: usize,
    rendezvous: String,
    broker_index: usize,
    workers: usize,
    batches: usize,
    barrier: Arc<Barrier>,
    semaphore: Arc<Semaphore>,
    worker_index: usize,
) {
    println!("[{}] Loading assets..", worker_index);

    let membership = Membership::load_exact("/home/ubuntu/assets/membership.pod", servers);

    println!("[{}] Loading batches..", worker_index);

    let broker_offset = broker_index * (workers * batches);
    let worker_offset = worker_index * batches;
    let offset = broker_offset + worker_offset;

    let range = offset..(offset + batches);

    println!("[{}] Batches range: {:?}", worker_index, range);

    let compressed_batches = range
        .into_iter()
        .map(|index| {
            let path = format!("/home/ubuntu/assets/batches/{:06}.pod", index);
            let bytes = fs::read(path.as_str()).unwrap();

            let compressed_batch =
                bincode::deserialize::<CompressedBatch>(bytes.as_slice()).unwrap();

            let batch = compressed_batch.decompress();

            let compressed_batch =
                bincode::deserialize::<CompressedBatch>(bytes.as_slice()).unwrap();

            let root = batch.root();

            (root, compressed_batch)
        })
        .collect::<Vec<_>>();

    println!("[{}] Booting worker..", worker_index);

    let connector =
        RendezvousConnector::new(rendezvous.clone(), KeyChain::random(), Default::default());

    let connector = SessionConnector::new(connector);

    let broker = LoadBroker::new(membership, connector, compressed_batches);

    barrier.wait().await;

    if worker_index == 0 {
        println!("All workers ready.");

        let rendezvous_client = RendezvousClient::new(rendezvous, Default::default());

        rendezvous_client
            .publish_card(KeyChain::random().keycard(), Some(1))
            .await
            .unwrap(); // Second shard is for brokers

        println!("Waiting for servers..");

        while rendezvous_client.get_shard(0).await.is_err() {
            time::sleep(Duration::from_secs(1)).await;
        }

        println!("Waiting for brokers..");

        while rendezvous_client.get_shard(1).await.is_err() {
            time::sleep(Duration::from_secs(1)).await;
        }

        println!("Triggering workers..");

        semaphore.add_permits(workers);
    }

    let _permit = semaphore.acquire().await.unwrap();

    println!("[{}] Broadcasting..", worker_index);

    for index in 0..batches {
        broker.broadcast(index).await;
    }

    println!("[{}] Broadcasting completed.", worker_index);
}
