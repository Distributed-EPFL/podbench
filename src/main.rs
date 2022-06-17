mod broker;
mod rendezvous;
mod server;

use broker::broker;
use rendezvous::rendezvous;
use server::server;

#[tokio::main]
async fn main() {
    let args = lapp::parse_args(
        "
        Welcome to `pod`'s benchmark framework. Let's make TOB go brrrr!
        
        Required for all roles:
          <role> (string) one of `rendezvous`, `server` or `broker`
          --servers (integer) total number of servers (*)

        Required for `rendezvous`
          --brokers (integer) total number of brokers

        Required for `server` and `broker`:
          --rendezvous (string) IP address of the rendezvous node (*)
          --index (integer) index of the server / broker

        Options for `server`:
          --loopback use `LoopBack` broadcast instead of `HotStuff` (*)

        Required for `broker`:
          --workers (integer) number of workers per broker (*)
          --batches (integer) number of batches to send (*)

        (*) This parameter must match at all applicable nodes
        ",
    );

    match args.get_string("role").as_str() {
        "rendezvous" => {
            let servers = args.get_integer("servers") as usize;
            let brokers = args.get_integer("brokers") as usize;

            rendezvous(servers, brokers).await;
        }
        "server" => {
            let servers = args.get_integer("servers") as usize;
            let rendezvous = args.get_string("rendezvous");
            let index = args.get_integer("index") as usize;
            let loopback = args.get_bool("loopback");

            server(servers, rendezvous, index, loopback).await;
        }
        "broker" => {
            let servers = args.get_integer("servers") as usize;
            let rendezvous = args.get_string("rendezvous");
            let index = args.get_integer("index") as usize;
            let workers = args.get_integer("workers") as usize;
            let batches = args.get_integer("batches") as usize;

            broker(servers, rendezvous, index, workers, batches).await;
        }
        _ => {
            println!("podbench error: argument #1 'role': must be either 'rendezvous', 'server' or 'broker'");
            println!("Type podbench --help for more information");
        }
    }
}
