use std::net::ToSocketAddrs;

use error::Result;
use may::coroutine::JoinHandle;
use may::net::{TcpListener, TcpStream};
use native_tls::{TlsConnector, TlsStream};
use tungstenite::client::client;
use tungstenite::handshake::client::Request;
use tungstenite::server::accept;
use tungstenite::{Message, WebSocket};
use url::Url;
pub fn run_websocket_server<T: ToSocketAddrs>(address: T) -> JoinHandle<()> {
    let address = address
        .to_socket_addrs()
        .expect("invalid address")
        .next()
        .expect("can't resolve address");

    go!(move || {
        let listener = TcpListener::bind(address).unwrap();
        // for stream in listener.incoming() {
        while let Ok((stream, _)) = listener.accept() {
            go!(move || -> () {
                let mut websocket = accept(stream).expect("ws failed to accept");

                loop {
                    let msg = match websocket.read_message() {
                        Ok(msg) => msg,
                        Err(e) => {
                            error!("{}", e.to_string());
                            break;
                        }
                    };

                    // Just echo back everything that the client sent to us
                    if msg.is_binary() || msg.is_text() {
                        websocket.write_message(msg).expect("ws failed to write");
                    }
                }
            });
        }
    })
}

pub struct WsClient {
    client: WebSocket<TcpStream>,
}

impl WsClient {
    pub fn new<T: ToSocketAddrs>(address: T) -> Result<Self> {
        let stream = TcpStream::connect(address)?;
        let url = Url::parse("wss://localhost/")?;
        let req = Request::from(url);

        let (client, _) = client(req, stream)?;
        Ok(WsClient { client })
    }

    pub fn send_message(&mut self, msg: String) -> Result<()> {
        self.client.write_message(Message::Text(msg))?;
        Ok(())
    }

    pub fn recv_message(&mut self) -> Result<String> {
        let msg = self.client.read_message()?;

        match msg {
            Message::Text(s) => Ok(s),
            _ => bail!("not a text message"),
        }
    }

    pub fn close(mut self) -> Result<()> {
        self.client.close(None)?;
        Ok(())
    }
}

// this is only for test client
// for test server we have to setup the server encrypt pub/priv keys
pub struct WssClient {
    client: WebSocket<TlsStream<TcpStream>>,
}

impl WssClient {
    pub fn new(host: &str) -> Result<Self> {
        let stream = TcpStream::connect((host, 443))?;
        let connector = TlsConnector::builder()?.build()?;
        let stream = connector.connect(host, stream)?;
        let url = format!("wss://{}/", host);
        let url = Url::parse(&url)?;
        let req = Request::from(url);

        let (client, _) = client(req, stream)?;
        Ok(WssClient { client })
    }

    pub fn send_message(&mut self, msg: String) -> Result<()> {
        self.client.write_message(Message::Text(msg))?;
        Ok(())
    }

    pub fn recv_message(&mut self) -> Result<String> {
        let msg = self.client.read_message()?;
        match msg {
            Message::Text(s) => Ok(s),
            _ => bail!("not a text message"),
        }
    }

    pub fn close(mut self) -> Result<()> {
        self.client.close(None)?;
        Ok(())
    }
}
