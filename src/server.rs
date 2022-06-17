use pod::{Directory, HotStuff, LoopBack, Membership, Passepartout, Server};

use std::time::{Duration, Instant};

use talk::{
    link::rendezvous::{Client as RendezvousClient, Listener as RendezvousListener},
    net::SessionListener,
};

pub(crate) async fn server(servers: usize, rendezvous: String, index: usize, loopback: bool) {
    println!("Loading assets..");

    let passepartout = Passepartout::load("/home/ubuntu/assets/passepartout.pod");
    let membership = Membership::load_exact("/home/ubuntu/assets/membership.pod", servers);
    let directory = Directory::load("/home/ubuntu/assets/directory.pod");

    println!("Booting server..");

    let servers = membership.servers().keys().collect::<Vec<_>>();
    let identity = servers[index].clone();

    let keychain = passepartout.keychain(identity);
    let keycard = keychain.keycard();

    let listener =
        RendezvousListener::new(rendezvous.clone(), keychain.clone(), Default::default()).await;

    let listener = SessionListener::new(listener);

    let mut server = if loopback {
        Server::new(keychain, membership, directory, LoopBack::new(), listener)
    } else {
        Server::new(
            keychain,
            membership,
            directory,
            HotStuff::connect(&"127.0.0.1:7000".parse().unwrap())
                .await
                .unwrap(),
            listener,
        )
    };

    println!("Announcing server..");

    let rendezvous_client = RendezvousClient::new(rendezvous, Default::default());

    rendezvous_client
        .publish_card(keycard, Some(0)) // First shard is for servers
        .await
        .unwrap();

    println!("Server running.");

    let mut count = 0;
    let mut last_count = 0;

    let mut start = None;
    let mut last_refresh = Instant::now();

    loop {
        let _batch = server.next_batch().await;

        if start.is_none() {
            start = Some(Instant::now());
        }

        let start = start.unwrap();
        count += 1;

        if last_refresh.elapsed() > Duration::from_secs(1) {
            println!(
                "[{:.02} s] {} B ({} B / s instant, {} B / s average).",
                start.elapsed().as_secs_f64(),
                count,
                (((count - last_count) as f64) / last_refresh.elapsed().as_secs_f64()),
                ((count as f64) / start.elapsed().as_secs_f64())
            );

            last_count = count;
            last_refresh = Instant::now();
        }
    }
}
