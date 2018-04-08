#[macro_use]
extern crate may;
extern crate tungstenite;

mod network;

#[cfg(test)]
mod tests {
    use super::network::*;
    #[test]
    fn it_works() {
        run_websocket_server();
        assert_eq!(2 + 2, 4);
    }
}
