// use std::io::Read;
use std::marker::PhantomData;
use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use error::Result;
use may::coroutine::JoinHandle;
use may::net::{TcpListener, TcpStream};
use may::sync::{AtomicOption, RwLock};
use may_waiter::WaiterMap;
use serde_json::{self, Value};
use tungstenite::protocol::Role;
use tungstenite::server::accept;
use tungstenite::{Message, WebSocket};

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
pub trait Server<T> {
    fn on_message(ws: Arc<WsConnection<T>>, subject: String, body: Value) -> Result<()>;
    fn on_request(ws: Arc<WsConnection<T>>, command: String, params: Value) -> Result<Value>;
}

pub trait Sender {
    fn send_json(&self, value: Value) -> Result<()>;

    fn send_message(&self, kind: &str, content: Value) -> Result<()> {
        self.send_json(json!([kind, &content]))
    }

    fn send_just_saying(&self, subject: &str, body: Value) -> Result<()> {
        self.send_message("justsaying", json!({ "subject": subject, "body": body }))
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
        self.send_result(json!({ "unit": unit, "result": "error", "error": error }))
    }

    fn send_response(&self, tag: &str, response: Value) -> Result<()> {
        if response.is_null() {
            return self.send_message("response", json!({ "tag": tag }));
        }
        self.send_message("response", json!({ "tag": tag, "response": response }))
    }

    fn send_error_response(&self, tag: &str, error: Value) -> Result<()> {
        self.send_response(tag, json!({ "error": error }))
    }
}

struct WsInner {
    ws: WebSocket<TcpStream>,
    last_recv: Instant,
}

pub struct WsConnection<T> {
    // lock proected inner
    ws: RwLock<WsInner>,
    // peer name is never changed once init
    peer: String,
    // the waiting request
    req_map: Arc<WaiterMap<usize, Value>>,
    // the listening coroutine
    listener: AtomicOption<JoinHandle<()>>,
    // the actual state data
    data: T,
    // for request unique id generation
    id: AtomicUsize,
}

impl<T> Sender for WsConnection<T> {
    fn send_json(&self, value: Value) -> Result<()> {
        let msg = serde_json::to_string(&value)?;
        if msg.len() < 1000 {
            debug!("SENDING to {}: {}", self.peer, msg);
        } else {
            debug!("SENDING to {}: huge message", self.peer);
        }

        let mut g = self.ws.write().unwrap();
        g.ws.write_message(Message::Text(msg))?;
        Ok(())
    }
}

impl<T> WsConnection<T> {
    // use &String instead of &str for db
    pub fn get_peer(&self) -> &String {
        &self.peer
    }

    pub fn get_last_recv_tm(&self) -> Instant {
        let g = self.ws.read().unwrap();
        g.last_recv
    }

    pub fn set_last_recv_tm(&self, time: Instant) {
        let mut g = self.ws.write().unwrap();
        g.last_recv = time;
    }

    pub fn get_data(&self) -> &T {
        &self.data
    }
}

impl<T> Drop for WsConnection<T> {
    fn drop(&mut self) {
        if ::std::thread::panicking() {
            return;
        }
        if let Some(h) = self.listener.take(Ordering::Relaxed) {
            // close the connection first
            self.ws.write().unwrap().ws.close(None).ok();
            unsafe { h.coroutine().cancel() };
            h.join().ok();
        }
    }
}

impl<T> WsConnection<T> {
    /// create a client from stream socket
    pub fn new(ws: WebSocket<TcpStream>, data: T, peer: String, role: Role) -> Result<Arc<Self>>
    where
        T: Server<T> + Send + Sync + 'static,
    {
        let req_map = Arc::new(WaiterMap::new());

        let req_map_1 = req_map.clone();
        let mut reader = WebSocket::from_raw_socket(ws.get_ref().try_clone()?, role, None);
        let ws = Arc::new(WsConnection {
            ws: RwLock::new(WsInner {
                ws,
                last_recv: Instant::now(),
            }),
            peer,
            req_map,
            listener: AtomicOption::none(),
            data,
            id: AtomicUsize::new(0),
        });

        // we can't have a strong ref in the driver coroutine!
        // or it will never got dropped
        let ws_1 = Arc::downgrade(&ws);

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

                let mut value: Value = t_c!(serde_json::from_str(&msg));
                let msg_type = value[0].take();
                let msg_type = t_c!(msg_type.as_str().ok_or("no msg type"));

                // we use weak ref here, need to upgrade to check if dropped
                let ws = match ws_1.upgrade() {
                    Some(c) => c,
                    None => return,
                };
                if msg.len() < 1000 {
                    debug!("RECV from {}: {}", ws.peer, msg);
                } else {
                    debug!("RECV from {}: huge message!", ws.peer);
                }

                ws.set_last_recv_tm(Instant::now());

                match msg_type {
                    "justsaying" => {
                        #[derive(Deserialize)]
                        struct JustSaying {
                            subject: String,
                            #[serde(default)]
                            body: Value,
                        };
                        let JustSaying { subject, body } =
                            t_c!(serde_json::from_value(value[1].take()));
                        go!(move || if let Err(e) = T::on_message(ws, subject, body) {
                            error!("{}", e);
                        });
                    }
                    "request" => {
                        #[derive(Deserialize)]
                        struct Request {
                            command: String,
                            tag: String,
                            #[serde(default)]
                            params: Value,
                        };
                        let Request {
                            command,
                            tag,
                            params,
                        } = t_c!(serde_json::from_value(value[1].take()));
                        go!(move || {
                            // need to get and set the tag!!
                            match T::on_request(ws.clone(), command, params) {
                                Ok(rsp) => {
                                    // send the response
                                    t!(ws.send_response(&tag, rsp));
                                }
                                Err(e) => {
                                    error!("{:?}", e);
                                    let error = json!(e.to_string());
                                    t!(ws.send_error_response(&tag, error));
                                }
                            }
                        });
                    }
                    "response" => {
                        // set the wait req
                        let tag = match value[1]["tag"].as_str() {
                            Some(t) => t
                                .parse()
                                .unwrap_or_else(|e| panic!("tag {:?} is not u64! err={}", t, e)),
                            None => {
                                error!("tag is not found for response");
                                continue;
                            }
                        };
                        debug!("got response for tag={}", tag);
                        req_map_1.set_rsp(&tag, value).ok();
                    }
                    s => {
                        error!("unkonw msg type: {}", s);
                        continue;
                    }
                }
            }
        });

        ws.listener.swap(listener, Ordering::Relaxed);
        Ok(ws)
    }

    pub fn send_request(&self, command: &str, param: &Value) -> Result<Value> {
        let mut request = match param {
            Value::Null => json!({ "command": command }),
            _ => json!({"command": command, "params": param}),
        };
        let tag = self.id.fetch_add(1, Ordering::Relaxed);
        request["tag"] = json!(tag.to_string());

        let blocker = self.req_map.new_waiter(tag);
        self.send_message("request", request)?;

        let timeout = Some(Duration::from_secs(::config::STALLED_TIMEOUT as u64));
        #[derive(Deserialize)]
        struct Response {
            #[allow(dead_code)]
            tag: String,
            #[serde(default)]
            response: Value,
        };

        let rsp: Response = serde_json::from_value(blocker.wait_rsp(timeout)?[1].take())?;
        if !rsp.response["error"].is_null() {
            bail!("{} err: {}", command, rsp.response["error"]);
        }
        Ok(rsp.response)
    }

    #[inline]
    pub fn conn_eq(&self, other: &WsConnection<T>) -> bool {
        ::std::ptr::eq(self, other)
    }
}

// helper struct for easy use
pub struct WsServer<T>(PhantomData<T>);

impl<T> WsServer<T> {
    // f is used to save the connection globally
    pub fn start<A, F>(address: A, f: F) -> JoinHandle<()>
    where
        A: ToSocketAddrs,
        F: Fn(Arc<WsConnection<T>>) + Send + 'static,
        T: Server<T> + Default + Send + Sync + 'static,
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
                    Ok(addr) => addr.to_string(),
                    Err(_) => "unknown peer".to_owned(),
                };
                let ws = t_c!(accept(stream));
                let ws = t_c!(WsConnection::new(ws, T::default(), peer, Role::Server));
                f(ws);
            }
        })
    }
}
