use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::network::{Sender, Server, WsConnection};
use config;
use db;
use error::Result;
use joint::Joint;
use joint_storage;
use map_lock::MapLock;
use may::coroutine;
use may::net::TcpStream;
use may::sync::RwLock;
use rusqlite::Connection;
use serde_json::{self, Value};
use storage;
use tungstenite::client::client;
use tungstenite::handshake::client::Request;
use tungstenite::protocol::Role;
use url::Url;
use validation;

pub struct HubData {
    // indicate if this connection is a subscribed peer
    is_subscribed: AtomicBool,
    is_source: AtomicBool,
}

pub type HubConn = WsConnection<HubData>;

// global data that record the internal state
lazy_static! {
    // global Ws connections
    pub static ref WSS: WsConnections = WsConnections::new();
    // maybe this is too heavy, could use an optimized hashset<AtomicBool>
    static ref UNIT_IN_WORK: MapLock<String> = MapLock::new();
}

fn init_connection(ws: &Arc<HubConn>) {
    use rand::{thread_rng, Rng};

    t!(ws.send_version());
    t!(ws.send_subscribe());
    t!(ws.send_hub_challenge());

    let mut rng = thread_rng();
    let n: u64 = rng.gen_range(0, 1000);
    let ws = Arc::downgrade(ws);
    go!(move || loop {
        coroutine::sleep(Duration::from_millis(3000 + n));
        let ws = match ws.upgrade() {
            Some(ws) => ws,
            None => return,
        };
        if ws.get_last_recv_tm().elapsed() < Duration::from_secs(5) {
            continue;
        }
        // heartbeat failed so just close the connnection
        let rsp = ws.send_heartbeat();
        if rsp.is_err() {
            error!("heartbeat err= {}", rsp.unwrap_err());
            ws.close();
        }
    });
}

// global request has no specific ws connections, just find a proper one should be fine
pub struct WsConnections {
    inbound: RwLock<Vec<Arc<HubConn>>>,
    outbound: RwLock<Vec<Arc<HubConn>>>,
    next_inbound: AtomicUsize,
    next_outbound: AtomicUsize,
}

impl WsConnections {
    fn new() -> Self {
        WsConnections {
            inbound: RwLock::new(Vec::new()),
            outbound: RwLock::new(Vec::new()),
            next_inbound: AtomicUsize::new(0),
            next_outbound: AtomicUsize::new(0),
        }
    }

    pub fn add_inbound(&self, inbound: Arc<HubConn>) {
        init_connection(&inbound);
        let mut g = self.inbound.write().unwrap();
        g.push(inbound);
    }

    pub fn add_outbound(&self, outbound: Arc<HubConn>) {
        init_connection(&outbound);
        let mut g = self.outbound.write().unwrap();
        g.push(outbound);
    }

    pub fn close_all(&self) {
        let mut g = self.outbound.write().unwrap();
        g.clear();
        let mut g = self.inbound.write().unwrap();
        g.clear();
    }

    pub fn close(&self, conn: &HubConn) {
        // find out the actor and remove it
        let mut g = self.outbound.write().unwrap();
        for i in 0..g.len() {
            if g[i].conn_eq(&conn) {
                g.swap_remove(i);
                return;
            }
        }

        let mut g = self.inbound.write().unwrap();
        for i in 0..g.len() {
            if g[i].conn_eq(&conn) {
                g.swap_remove(i);
                return;
            }
        }
    }

    pub fn get_next_inbound(&self) -> Arc<HubConn> {
        let g = self.inbound.read().unwrap();
        let len = g.len();
        assert_ne!(len, 0);
        let idx = self.next_inbound.fetch_add(1, Ordering::Relaxed) % len;
        g[idx].clone()
    }

    pub fn get_next_outbound(&self) -> Arc<HubConn> {
        let g = self.outbound.read().unwrap();
        let len = g.len();
        assert_ne!(len, 0);
        let idx = self.next_outbound.fetch_add(1, Ordering::Relaxed) % len;
        g[idx].clone()
    }
}

impl Server<HubData> for HubData {
    fn new() -> HubData {
        HubData {
            is_subscribed: AtomicBool::new(false),
            is_source: AtomicBool::new(false),
        }
    }

    fn on_message(ws: Arc<HubConn>, subject: String, body: Value) -> Result<()> {
        match subject.as_str() {
            "version" => ws.on_version(body)?,
            "hub/challenge" => ws.on_hub_challenge(body)?,
            "free_joints_end" => {} // not handled
            "error" => error!("recevie error: {}", body),
            "info" => info!("recevie info: {}", body),
            "result" => info!("recevie result: {}", body),
            "joint" => ws.on_joint(body)?,
            subject => bail!("on_message unkown subject: {}", subject),
        }
        Ok(())
    }

    fn on_request(ws: Arc<HubConn>, command: String, params: Value) -> Result<Value> {
        let response = match command.as_str() {
            "heartbeat" => ws.on_heartbeat(params)?,
            "subscribe" => ws.on_subscribe(params)?,
            command => bail!("on_request unkown command: {}", command),
        };
        Ok(response)
    }
}

// internal state access
impl HubConn {
    pub fn is_subscribed(&self) -> bool {
        let data = self.get_data();
        data.is_subscribed.load(Ordering::Relaxed)
    }

    fn set_subscribed(&self) {
        let data = self.get_data();
        data.is_subscribed.store(true, Ordering::Relaxed);
    }

    pub fn is_source(&self) -> bool {
        let data = self.get_data();
        data.is_source.load(Ordering::Relaxed)
    }

    fn set_source(&self) {
        let data = self.get_data();
        data.is_source.store(true, Ordering::Relaxed);
    }
}

// the server side impl
impl HubConn {
    fn on_version(&self, version: Value) -> Result<()> {
        if version["protocol_version"].as_str() != Some(config::VERSION) {
            error!("Incompatible versions, mine {}", config::VERSION);
            self.close();
        }

        if version["alt"].as_str() != Some(config::ALT) {
            error!("Incompatible alt, mine {}", config::ALT);
            self.close();
        }

        info!("got peer version: {}", version);
        Ok(())
    }

    fn on_heartbeat(&self, _: Value) -> Result<Value> {
        Ok(Value::Null)
    }

    fn on_subscribe(&self, param: Value) -> Result<Value> {
        // TODO: is it necessary to detect the self connection? (#63)
        let _subscription_id = param["subscription_id"]
            .as_str()
            .ok_or(format_err!("no subscription_id"))?;

        self.set_subscribed();
        Ok(json!("subscribed"))
    }

    fn on_hub_challenge(&self, param: Value) -> Result<()> {
        // this is hub, we do nothing here
        // only wallet would save the challenge and save the challenge
        // for next login and match
        info!("peer is a hub, challenge = {}", param);
        Ok(())
    }

    fn on_joint(&self, param: Value) -> Result<()> {
        let joint: Joint = serde_json::from_value(param)?;
        info!("receive a joint: {:?}", joint);
        ensure!(joint.unit.unit.is_some(), "no unit");
        let mut db = db::DB_POOL.get_connection();
        {
            let mut stmt = db.prepare_cached(
                "SELECT 1 FROM archived_joints WHERE unit=? AND reason='uncovered'",
            )?;

            if stmt.exists(&[joint.unit.unit.as_ref().unwrap()])? {
                return self.send_error(json!("this unit is already known and archived"));
            }
        }
        self.handle_online_joint(joint, &mut db)
    }
}

impl HubConn {
    fn handle_online_joint(&self, mut joint: Joint, db: &mut Connection) -> Result<()> {
        use joint_storage::CheckNewResult;
        use validation::{ValidationError, ValidationOk};

        // clear the main chain index
        joint.unit.main_chain_index = None;
        let unit = joint.unit.unit.as_ref().unwrap();
        // check if unit is in work, when g is dropped unlock the unit
        let g = UNIT_IN_WORK.try_lock(vec![unit.to_owned()]);
        if g.is_none() {
            // the unit is in work, do nothing
            return Ok(());
        }

        match joint_storage::check_new_joint(db, &joint)? {
            CheckNewResult::New => {
                // do nothing here, proceed to valide
            }
            CheckNewResult::Known => {
                if joint.unsigned == Some(true) {
                    bail!("known unsigned");
                }
                self.send_result(json!({"unit": unit, "result": "known"}))?;
                self.write_event("know_good")?;
            }
            CheckNewResult::KnownBad => {
                self.send_result(json!({"unit": unit, "result": "known_bad"}))?;
                self.write_event("know_bad")?;
            }

            CheckNewResult::KnownUnverified => {
                self.send_result(json!({"unit": unit, "result": "known_unverified"}))?
            }
        }

        match validation::validate(db, &joint) {
            Ok(ok) => match ok {
                ValidationOk::Unsigned(_) => {
                    if joint.unsigned != Some(true) {
                        bail!("ifOkUnsigned() signed");
                    }
                }
                ValidationOk::Signed(_, _) => {
                    if joint.unsigned == Some(true) {
                        bail!("ifOk() unsigned");
                    }
                    joint.save()?;
                    // self.validation_unlock()?;
                    // self.send_result(json!({"unit": unit, "result": "accepted"}))?;
                    // // forward to other peers
                    // if (!bCatchingUp) {
                    //     self.forwardJoint(ws, objJoint)?;
                    // }
                    // drop(g);
                    // // wake up other joints that depend on me
                    // self.findAndHandleJointsThatAreReady(unit);
                    unimplemented!();
                    // TODO: forward to other peers
                    // wake up other joints that depend on me
                    // self.find_and_handle_joints_that_are_ready(unit)?;
                }
            },
            Err(err) => {
                let err: ValidationError = err.downcast()?;
                match err {
                    ValidationError::UnitError { err } => {
                        warn!("{} validation failed: {}", unit, err);
                        self.send_error_result(unit, &err)?;
                        self.purge_joint_and_dependencies_and_notify_peers(&joint, &err)?;
                        if !err.contains("authentifier verification failed")
                            && !err.contains("bad merkle proof at path")
                        {
                            self.write_event("invalid")?;
                        }
                    }
                    ValidationError::JointError { err } => {
                        self.send_error_result(unit, &err)?;
                        self.write_event("invalid")?;
                        // TODO: insert known_bad_jonts
                        unimplemented!()
                        // b.query(
                        // 	"INSERT INTO known_bad_joints (joint, json, error) VALUES (?,?,?)",
                        // 	[objectHash.getJointHash(objJoint), JSON.stringify(objJoint), error],
                        // 	function(){
                        // 		delete assocUnitsInWork[unit];
                        // 	}
                        // );
                    }
                    ValidationError::NeedHashTree => {
                        info!("need hash tree for unit {}", unit);
                        if joint.unsigned == Some(true) {
                            bail!("need hash tree unsigned");
                        }
                        unimplemented!()
                        // if !bCatchingUp && !bWaitingForCatchupChain {
                        //     self.request_catchup()?;
                        // }
                    }
                    ValidationError::NeedParentUnits(missing_units) => {
                        let info = format!("unresolved dependencies: {}", missing_units.join(", "));
                        self.send_info(json!({"unit": unit, "info": info}))?;
                        joint_storage::save_unhandled_joint_and_dependencies(
                            &db,
                            &joint,
                            &missing_units,
                            self.get_peer(),
                        )?;
                        drop(g);
                        self.request_new_missing_joints(&db, &missing_units)?;
                    }
                    ValidationError::TransientError { err } => bail!(err),
                }
            }
        }

        Ok(())
    }

    // record peer event in database
    fn write_event(&self, event: &str) -> Result<()> {
        // TODO: write event to database to record if the peer is evil
        let _ = event;
        Ok(())
    }

    fn purge_joint_and_dependencies_and_notify_peers(
        &self,
        joint: &Joint,
        err: &str,
    ) -> Result<()> {
        let _ = (joint, err);
        unimplemented!()
    }

    #[allow(dead_code)]
    fn request_catchup(&self) -> Result<()> {
        unimplemented!()
    }

    fn request_new_missing_joints(&self, db: &Connection, units: &[String]) -> Result<()> {
        let mut new_units = Vec::new();

        for unit in units {
            let g = UNIT_IN_WORK.try_lock(vec![unit.clone()]);
            if g.is_none() {
                continue;
            }
            if self.have_pending_joint_request(unit) {
                info!("unit {} was already requested", unit);
                continue;
            }

            use joint_storage::CheckNewResult;
            match joint_storage::check_new_unit(db, unit)? {
                CheckNewResult::New => {
                    new_units.push(unit.clone());
                }
                _ => info!("unit {} is already known", unit),
            }
        }
        // TODO: need to re-check if unit is on processing #85
        if !new_units.is_empty() {
            self.request_joints(&new_units)?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn notify_watchers(&self, joint: &Joint) -> Result<()> {
        let _ = joint;
        unimplemented!()
    }

    #[allow(dead_code)]
    fn have_pending_joint_request(&self, unit: &String) -> bool {
        let _ = unit;
        unimplemented!()
    }
}

// the client side impl
impl HubConn {
    fn send_version(&self) -> Result<()> {
        // TODO: read these things from config
        self.send_just_saying(
            "version",
            json!({
                "protocol_version": config::VERSION,
                "alt": config::ALT,
                "library": "rust-trustnote",
                "library_version": "0.1.0",
                "program": "rust-trustnote-hub",
                "program_version": "0.1.0"
            }),
        )
    }

    fn send_hub_challenge(&self) -> Result<()> {
        use object_hash;
        let challenge = object_hash::gen_random_string(30);
        self.send_just_saying("hub/challenge", json!(challenge))?;
        Ok(())
    }

    fn send_subscribe(&self) -> Result<()> {
        use object_hash;
        // TODO: this is used to detect self-connect (#63)
        let subscription_id = object_hash::gen_random_string(30);
        let db = ::db::DB_POOL.get_connection();
        let last_mci = storage::read_last_main_chain_index(&db)?;
        self.send_request(
            "subscribe",
            json!({ "subscription_id": subscription_id, "last_mci": last_mci}),
        )?;

        self.set_source();
        Ok(())
    }

    fn send_heartbeat(&self) -> Result<()> {
        self.send_request("heartbeat", Value::Null)?;
        Ok(())
    }

    // remove self from global
    fn close(&self) {
        info!("close connection: {}", self.get_peer());
        WSS.close(self);
    }

    fn request_joints(&self, units: &[String]) -> Result<()> {
        // TODO: #83
        let _ = units;
        unimplemented!()
    }
}

pub fn create_outbound_conn<A: ToSocketAddrs>(address: A) -> Result<Arc<HubConn>> {
    let stream = TcpStream::connect(address)?;
    let peer = match stream.peer_addr() {
        Ok(addr) => format!("{}", addr),
        Err(_) => "unknown peer".to_owned(),
    };
    let url = Url::parse("wss://localhost/")?;
    let req = Request::from(url);
    let (conn, _) = client(req, stream)?;
    // let ws
    let ws = WsConnection::new(conn, HubData::new(), peer, Role::Client)?;

    WSS.add_outbound(ws.clone());
    Ok(ws)
}
