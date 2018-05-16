// use std::io::Read;
use std::marker::PhantomData;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use error::Result;
use may::coroutine::JoinHandle;
use may::net::{TcpListener, TcpStream};
use may::sync::Mutex;
use may_waiter::WaiterMap;
use serde_json::{self, Value};
use tungstenite::protocol::Role;
use tungstenite::server::accept;
use tungstenite::{Message, WebSocket};

macro_rules! t {
    ($e:expr) => {
        match $e {
            Ok(val) => val,
            Err(err) => {
                error!("call = {:?}\nerr = {:?}", stringify!($e), err);
            }
        }
    };
}

macro_rules! t_c {
    ($e:expr) => {
        match $e {
            Ok(val) => val,
            Err(err) => {
                error!("call = {:?}\nerr = {:?}", stringify!($e), err);
                continue;
            }
        }
    };
}

// the server part trait
pub trait Server {
    fn on_message(&self, msg: Value) -> Result<()>;
    fn on_request(&self, msg: Value) -> Result<Value>;
    // need to close the connection from global
    fn close(&self, ws: Arc<WsWrapper>);
}

pub trait Sender {
    fn send_json(&self, value: Value) -> Result<()>;

    fn send_message(&self, kind: &str, content: Value) -> Result<()> {
        self.send_json(json!([kind, &content]))
    }

    fn send_just_saying(&self, subject: &str, body: Value) -> Result<()> {
        self.send_message("justsaying", json!({"subject": subject, "body": body}))
    }

    fn send_error(&self, error: Value) -> Result<()> {
        self.send_just_saying("error", error)
    }

    fn send_info(&self, info: Value) -> Result<()> {
        self.send_just_saying("info", info)
    }

    fn send_result(&self, result: Value) -> Result<()> {
        self.send_just_saying("result", result)
    }

    fn send_error_result(&self, unit: &str, error: &str) -> Result<()> {
        self.send_result(json!({"unit": unit, "result": "error", "error": error}))
    }

    fn send_response(&self, tag: &str, response: Value) -> Result<()> {
        self.send_message("response", json!({"tag": tag, "response": response}))
    }
}

pub struct WsWrapper {
    ws: Mutex<WebSocket<TcpStream>>,
    peer: String,
}

impl Drop for WsWrapper {
    fn drop(&mut self) {
        // send the close socket request
        let mut g = self.ws.lock().unwrap();
        g.close(None).ok();
    }
}

impl Sender for WsWrapper {
    fn send_json(&self, value: Value) -> Result<()> {
        let msg = serde_json::to_string(&value)?;
        info!("SENDING to {}: {}", self.peer, msg);
        let mut g = self.ws.lock().unwrap();
        g.write_message(Message::Text(msg))?;
        Ok(())
    }
}

pub struct WsConnection {
    // the connection write half
    pub ws: Arc<WsWrapper>,
    // the waiting request
    req_map: Arc<WaiterMap<String, Value>>,
    // the listening coroutine
    listener: Option<JoinHandle<()>>,
}

unsafe impl Send for WsConnection {}
unsafe impl Sync for WsConnection {}

impl Drop for WsConnection {
    fn drop(&mut self) {
        if ::std::thread::panicking() {
            return;
        }
        self.listener.take().map(|h| {
            unsafe { h.coroutine().cancel() };
            h.join().ok();
        });
    }
}

impl WsConnection {
    /// create a client from stream socket
    pub fn new<S>(ws: WebSocket<TcpStream>, server: S, peer: String, role: Role) -> Result<Self>
    where
        S: Server + Clone + Send + Sync + 'static,
    {
        let req_map = Arc::new(WaiterMap::<String, Value>::new());

        let req_map_1 = req_map.clone();
        let mut reader = WebSocket::from_raw_socket(ws.get_ref().try_clone()?, role);
        let ws = Arc::new(WsWrapper {
            ws: Mutex::new(ws),
            peer: peer,
        });
        let ws_1 = ws.clone();

        let listener = go!(move || {
            loop {
                let msg = match reader.read_message() {
                    Ok(msg) => msg,
                    Err(e) => {
                        error!("{}", e.to_string());
                        break;
                    }
                };

                let msg = match msg {
                    Message::Text(s) => s,
                    _ => {
                        error!("only text ws packet are supported");
                        continue;
                    }
                };
                debug!("receive msg: {}", msg);

                let value: Value = t_c!(serde_json::from_str(&msg));
                let msg_type = value[0].clone();
                let msg_type = t_c!(msg_type.as_str().ok_or("no msg type"));

                let ws = ws_1.clone();

                match msg_type {
                    "justsaying" => {
                        let server = server.clone();
                        go!(move || {
                            if let Err(e) = server.on_message(value) {
                                error!("{}", e);
                                if let Some(_) = e.downcast_ref::<::error::TrustnoteError>() {
                                    // need to close the connection
                                    // NOTE: it will dead lock if within the parent coroutine
                                    server.close(ws);
                                }
                            }
                        });
                    }
                    "request" => {
                        let server = server.clone();
                        go!(move || {
                            let tag = value[1]["tag"].clone();
                            if tag.as_str().is_none() {
                                return error!("tag is not found for request");
                            }
                            // need to get and set the tag!!
                            match server.on_request(value) {
                                Ok(rsp) => {
                                    // send the response
                                    t!(ws.send_response(tag.as_str().unwrap(), rsp));
                                }
                                Err(_e) => {
                                    let error = json!({});
                                    t!(ws.send_error(error));
                                }
                            }
                        });
                    }
                    "response" => {
                        // set the wait req
                        let tag = "asdf".to_owned();
                        req_map_1.set_rsp(&tag, value).ok();
                    }
                    s => {
                        error!("unkonw msg type: {}", s);
                        continue;
                    }
                }
            }
        });

        Ok(WsConnection {
            ws: ws,
            req_map: req_map,
            listener: Some(listener),
        })
    }

    pub fn send_request(&self, command: &str, param: Value) -> Result<Value> {
        let mut request = json!({"command": command, "params": param});
        let tag = ::object_hash::get_base64_hash(&request)?;
        request["tag"] = json!(tag);

        let blocker = self.req_map.new_waiter(tag);
        self.send_message("request", request)?;

        let timeout = Some(Duration::from_secs(::config::STALLED_TIMEOUT as u64));
        let rsp = blocker.wait_rsp(timeout)?;
        Ok(rsp)
    }

    pub fn ws_eq(&self, ws: &Arc<WsWrapper>) -> bool {
        Arc::ptr_eq(&self.ws, ws)
    }
}

impl Sender for WsConnection {
    fn send_json(&self, value: Value) -> Result<()> {
        self.ws.send_json(value)
    }
}

// helper struct for easy use
pub struct WsServer<S>(PhantomData<S>);

impl<S: Server + Clone + Send + Sync + 'static> WsServer<S> {
    // f is used to save the connection actor globally
    pub fn start<A, F>(address: A, server: S, f: F) -> JoinHandle<()>
    where
        A: ToSocketAddrs,
        F: Fn(WsConnection) + Send + 'static,
    {
        let address = address
            .to_socket_addrs()
            .expect("invalid address")
            .next()
            .expect("can't resolve address");

        go!(move || {
            let listener = TcpListener::bind(address).unwrap();
            // for stream in listener.incoming() {
            while let Ok((stream, _)) = listener.accept() {
                let peer = match stream.peer_addr() {
                    Ok(addr) => format!("{}", addr),
                    Err(_) => "unknown peer".to_owned(),
                };
                let ws = t_c!(accept(stream));
                let ws = t_c!(WsConnection::new(ws, server.clone(), peer, Role::Server));
                f(ws);
            }
        })
    }
}
