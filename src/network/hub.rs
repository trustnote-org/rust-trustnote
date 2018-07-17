use std::collections::VecDeque;
use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::network::{Sender, Server, WsConnection};
use atomic_lock::AtomicLock;
use catchup;
use config;
use db;
use error::Result;
use joint::Joint;
use joint_storage::{self, ReadyJoint};
use map_lock::MapLock;
use may::coroutine;
use may::net::TcpStream;
use may::sync::{Mutex, RwLock};
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
    static ref JOINT_IN_REQ: MapLock<String> = MapLock::new();
    static ref IS_CACTCHING_UP: AtomicLock = AtomicLock::new();
    // FIXME: should wait rust atomic number stable
    static ref COMING_ONLINE_TIME: AtomicUsize = AtomicUsize::new(::time::now() as usize);
}

fn init_connection(ws: &Arc<HubConn>) {
    use rand::{thread_rng, Rng};

    t!(ws.send_version());
    t!(ws.send_subscribe());
    t!(ws.send_hub_challenge());

    let mut rng = thread_rng();
    let n: u64 = rng.gen_range(0, 1000);
    let ws_c = Arc::downgrade(ws);

    // start the heartbeat timer for each connection
    go!(move || loop {
        coroutine::sleep(Duration::from_millis(3000 + n));
        let ws = match ws_c.upgrade() {
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
fn add_peer_host(bound: Arc<HubConn>) -> Result<()> {
    let peer = bound.get_peer();
    let v: Vec<&str> = peer.split(':').collect();
    if v[0].is_empty() {
        return Ok(());
    }
    let db = db::DB_POOL.get_connection();
    let mut stmt = db.prepare_cached("INSERT OR IGNORE INTO peer_hosts (peer_host) VALUES (?)")?;
    let host = v[0].to_string();
    stmt.execute(&[&host])?;
    Ok(())
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
        g.push(inbound.clone());
        drop(g);
        t!(add_peer_host(inbound));
    }

    pub fn add_outbound(&self, outbound: Arc<HubConn>) {
        init_connection(&outbound);
        let mut g = self.outbound.write().unwrap();
        g.push(outbound.clone());
        drop(g);
        t!(add_peer_host(outbound));
    }

    pub fn close_all(&self) {
        let mut g = self.outbound.write().unwrap();
        g.clear();
        let mut g = self.inbound.write().unwrap();
        g.clear();
    }

    fn get_ws(&self, conn: &HubConn) -> Arc<HubConn> {
        let g = self.outbound.read().unwrap();
        for c in &*g {
            if c.conn_eq(&conn) {
                return c.clone();
            }
        }
        drop(g);

        let g = self.inbound.read().unwrap();
        for c in &*g {
            if c.conn_eq(&conn) {
                return c.clone();
            }
        }

        unreachable!("can't find a ws connection from global wss!")
    }

    fn close(&self, conn: &HubConn) {
        // find out the actor and remove it
        let mut g = self.outbound.write().unwrap();
        for i in 0..g.len() {
            if g[i].conn_eq(&conn) {
                g.swap_remove(i);
                return;
            }
        }
        drop(g);

        let mut g = self.inbound.write().unwrap();
        for i in 0..g.len() {
            if g[i].conn_eq(&conn) {
                g.swap_remove(i);
                return;
            }
        }
    }

    pub fn get_next_inbound(&self) -> Option<Arc<HubConn>> {
        let g = self.inbound.read().unwrap();
        let len = g.len();
        if len == 0 {
            return None;
        }
        let idx = self.next_inbound.fetch_add(1, Ordering::Relaxed) % len;
        Some(g[idx].clone())
    }

    pub fn get_next_outbound(&self) -> Option<Arc<HubConn>> {
        let g = self.outbound.read().unwrap();
        let len = g.len();
        if len == 0 {
            return None;
        }
        let idx = self.next_outbound.fetch_add(1, Ordering::Relaxed) % len;
        Some(g[idx].clone())
    }

    pub fn get_next_peer(&self) -> Option<Arc<HubConn>> {
        self.get_next_outbound().or_else(|| self.get_next_inbound())
    }

    pub fn get_connection_by_name(&self, peer: &str) -> Option<Arc<HubConn>> {
        let g = self.outbound.read().unwrap();
        for c in &*g {
            if c.get_peer() == peer {
                return Some(c.clone());
            }
        }
        drop(g);

        let g = self.inbound.read().unwrap();
        for c in &*g {
            if c.get_peer() == peer {
                return Some(c.clone());
            }
        }

        None
    }

    fn request_free_joints(&self) {
        let g = self.outbound.read().unwrap();
        for ws in g.iter() {
            t!(ws.send_just_saying("refresh", Value::Null));
        }
    }

    fn forward_joint(&self, joint: &Joint) -> Result<()> {
        let outbound = self.outbound.read().unwrap().to_vec();
        for c in outbound {
            c.send_joint(joint)?;
        }

        let inbound = self.inbound.read().unwrap().to_vec();
        for c in inbound {
            c.send_joint(joint)?;
        }

        Ok(())
    }
}

impl Default for HubData {
    fn default() -> Self {
        HubData {
            is_subscribed: AtomicBool::new(false),
            is_source: AtomicBool::new(false),
        }
    }
}

impl Server<HubData> for HubData {
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
            "get_joint" => ws.on_get_joint(params)?,
            "catchup" => ws.on_catchup(params)?,
            "get_hash_tree" => ws.on_get_hash_tree(params)?,
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
            .ok_or_else(|| format_err!("no subscription_id"))?;

        self.set_subscribed();
        // send some joint in a background task
        let ws = WSS.get_ws(self);
        let last_mci = param["last_mci"].as_u64();
        try_go!(move || -> Result<()> {
            let db = db::DB_POOL.get_connection();
            if let Some(last_mci) = last_mci {
                ws.send_joints_since_mci(&db, last_mci as u32)?;
            } else {
                // TODO:
                // ws.send_free_joints()?;
            }
            Ok(())
        });
        Ok(json!("subscribed"))
    }

    fn on_hub_challenge(&self, param: Value) -> Result<()> {
        // this is hub, we do nothing here
        // only wallet would save the challenge and save the challenge
        // for next login and match
        info!("peer is a hub, challenge = {}", param);
        Ok(())
    }

    fn on_get_joint(&self, param: Value) -> Result<Value> {
        let unit = serde_json::from_value(param)?;
        let db = db::DB_POOL.get_connection();
        match storage::read_joint(&db, &unit) {
            Ok(joint) => Ok(serde_json::to_value(joint)?),
            Err(e) => {
                error!("read joint {} failed, err={}", unit, e);
                Ok(json!({ "joint_not_found": unit }))
            }
        }
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

    fn on_catchup(&self, param: Value) -> Result<Value> {
        let catchup_req: catchup::CatchupReq = serde_json::from_value(param)?;
        let db = db::DB_POOL.get_connection();
        let catchup_chain = catchup::prepare_catchup_chain(&db, catchup_req)?;
        Ok(serde_json::to_value(catchup_chain)?)
    }

    fn on_get_hash_tree(&self, param: Value) -> Result<Value> {
        let hash_tree_req: catchup::HashTreeReq = serde_json::from_value(param)?;
        let db = db::DB_POOL.get_connection();
        let hash_tree = catchup::read_hash_tree(&db, hash_tree_req)?;
        Ok(json!({ "balls": hash_tree }))
    }
}

impl HubConn {
    fn handle_online_joint(&self, mut joint: Joint, db: &mut Connection) -> Result<()> {
        use joint_storage::CheckNewResult;
        use validation::{ValidationError, ValidationOk};

        // clear the main chain index
        joint.unit.main_chain_index = None;
        let unit = joint.get_unit_hash();
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
                self.write_event(db, "know_good")?;
                return Ok(());
            }
            CheckNewResult::KnownBad => {
                self.send_result(json!({"unit": unit, "result": "known_bad"}))?;
                self.write_event(db, "know_bad")?;
                return Ok(());
            }

            CheckNewResult::KnownUnverified => {
                self.send_result(json!({"unit": unit, "result": "known_unverified"}))?;
                return Ok(());
            }
        }

        match validation::validate(db, &joint) {
            Ok(ok) => match ok {
                ValidationOk::Unsigned(_) => {
                    if joint.unsigned != Some(true) {
                        bail!("ifOkUnsigned() signed");
                    }
                }
                ValidationOk::Signed(validate_state, lock) => {
                    if joint.unsigned == Some(true) {
                        bail!("ifOk() unsigned");
                    }
                    joint.save(validate_state)?;
                    drop(lock);

                    self.send_result(json!({"unit": unit, "result": "accepted"}))?;

                    if !IS_CACTCHING_UP.is_locked() {
                        WSS.forward_joint(&joint)?;
                    }

                    // must release the guard to let other work continue
                    drop(g);

                    // wake up other joints that depend on me
                    find_and_handle_joints_that_are_ready(db, Some(unit))?;
                }
            },
            Err(err) => {
                match err {
                    ValidationError::OtherError { err } => {
                        error!("validation other err={}, unit={}", err, unit);
                    }

                    ValidationError::UnitError { err } => {
                        warn!("{} validation failed: {}", unit, err);
                        self.send_error_result(unit, &err)?;
                        self.purge_joint_and_dependencies_and_notify_peers(db, &joint, &err)?;
                        if !err.contains("authentifier verification failed")
                            && !err.contains("bad merkle proof at path")
                        {
                            self.write_event(db, "invalid")?;
                        }
                    }
                    ValidationError::JointError { err } => {
                        self.send_error_result(unit, &err)?;
                        self.write_event(db, "invalid")?;
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
                        // we are not saving the joint so that in case requestCatchup() fails,
                        // the joint will be requested again via findLostJoints,
                        // which will trigger another attempt to request catchup
                        try_go!(start_catchup);
                    }
                    ValidationError::NeedParentUnits(missing_units) => {
                        let info = format!("unresolved dependencies: {}", missing_units.join(", "));
                        self.send_info(json!({"unit": unit, "info": info}))?;
                        joint_storage::save_unhandled_joint_and_dependencies(
                            db,
                            &joint,
                            &missing_units,
                            self.get_peer(),
                        )?;
                        drop(g);
                        self.request_new_missing_joints(&db, &missing_units)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_saved_joint(
        &self,
        db: &mut Connection,
        joint: Joint,
        create_ts: usize,
        unhandled_joints: &mut VecDeque<ReadyJoint>,
    ) -> Result<()> {
        use joint_storage::CheckNewResult;
        use validation::{ValidationError, ValidationOk};

        let unit = joint.get_unit_hash();
        info!("handle_saved_joint: {}", unit);

        // check if unit is in work, when g is dropped unlock the unit
        let g = UNIT_IN_WORK.try_lock(vec![unit.to_owned()]);
        if g.is_none() {
            // the unit is in work, do nothing
            info!("handle_saved_joint: {} in work", unit);
            return Ok(());
        }

        match joint_storage::check_new_joint(db, &joint)? {
            CheckNewResult::New => {
                info!("new in handleSavedJoint: {}", unit);
                return Ok(());
            }
            CheckNewResult::Known => return Ok(()),
            CheckNewResult::KnownBad => return Ok(()),
            CheckNewResult::KnownUnverified => {}
        }
        match validation::validate(db, &joint) {
            Ok(ok) => match ok {
                ValidationOk::Unsigned(_) => {
                    if joint.unsigned != Some(true) {
                        bail!("ifOkUnsigned() signed");
                    }
                    joint_storage::remove_unhandled_joint_and_dependencies(db, unit)?;
                }
                ValidationOk::Signed(validation_state, lock) => {
                    if joint.unsigned == Some(true) {
                        bail!("ifOk() unsigned");
                    }
                    joint.save(validation_state)?;
                    drop(lock);

                    self.send_result(json!({"unit": unit, "result": "accepted"}))?;

                    const FORWARDING_TIMEOUT: u64 = 10 * 1000;
                    if !IS_CACTCHING_UP.is_locked()
                        && create_ts as u64 > ::time::now() - FORWARDING_TIMEOUT
                    {
                        WSS.forward_joint(&joint)?;
                    }
                    joint_storage::remove_unhandled_joint_and_dependencies(db, unit)?;
                    drop(g);

                    //Push back unit to DeQueue, later it can be popped and then the joints depend on it can get handled
                    let joints =
                        joint_storage::read_dependent_joints_that_are_ready(db, Some(unit))?;

                    for joint in joints {
                        unhandled_joints.push_back(joint);
                    }
                }
            },
            Err(err) => {
                match err {
                    ValidationError::OtherError { err } => {
                        error!("validation other err={}, unit={}", err, unit);
                    }
                    ValidationError::UnitError { err } => {
                        warn!("{} validation failed: {}", unit, err);
                        self.send_error_result(unit, &err)?;
                        self.purge_joint_and_dependencies_and_notify_peers(db, &joint, &err)?;
                        if !err.contains("authentifier verification failed")
                            && !err.contains("bad merkle proof at path")
                        {
                            self.write_event(db, "invalid")?;
                        }
                    }
                    ValidationError::JointError { err } => {
                        self.send_error_result(unit, &err)?;
                        self.write_event(db, "invalid")?;
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
                        bail!("handleSavedJoint: need hash tree");
                    }
                    ValidationError::NeedParentUnits(missing_units) => {
                        let miss_unit_set = missing_units
                            .iter()
                            .map(|s| format!("'{}'", s))
                            .collect::<Vec<_>>()
                            .join(",");
                        let sql = format!(
                            "SELECT 1 FROM archived_joints WHERE unit IN({}) LIMIT 1",
                            miss_unit_set
                        );
                        let mut stmt = db.prepare(&sql)?;
                        ensure!(
                            stmt.exists(&[])?,
                            "unit {} still has unresolved dependencies: {}",
                            unit,
                            miss_unit_set
                        );
                        info!(
                            "unit {} has unresolved dependencies that were archived: {}",
                            unit, miss_unit_set
                        );
                        drop(g);
                        self.request_new_missing_joints(&db, &missing_units)?;
                    }
                }
            }
        }

        Ok(())
    }

    // record peer event in database
    fn write_event(&self, db: &Connection, event: &str) -> Result<()> {
        if event.contains("invalid") || event.contains("nonserial") {
            let host = self.get_peer();
            let event_string: String = event.to_string();
            let column = format!("count_{}_joints", &event_string);
            let sql = format!(
                "UPDATE peer_hosts SET {}={}+1 WHERE peer_host=?",
                column, column
            );
            let mut stmt = db.prepare_cached(&sql)?;
            stmt.execute(&[host])?;

            let mut stmt =
                db.prepare_cached("INSERT INTO peer_events (peer_host, event) VALUES (?, ?)")?;
            stmt.execute(&[host, &event_string])?;
        }

        Ok(())
    }

    fn purge_joint_and_dependencies_and_notify_peers(
        &self,
        db: &mut Connection,
        joint: &Joint,
        err: &str,
    ) -> Result<()> {
        if err.contains("is not stable in view of your parents") {
            return Ok(());
        }
        joint_storage::purge_joint_and_dependencies(db, joint, err, |unit, peer, error| {
            if let Some(ws) = WSS.get_connection_by_name(peer) {
                let error = format!("error on (indirect) parent unit {}: {} ", unit, error);
                ws.send_error_result(unit, &error).ok();
            }
        })?;
        Ok(())
    }

    fn request_catchup(&self, db: &Connection) -> Result<()> {
        info!("will request catchup from {}", self.get_peer());

        // here we send out the real catchup request
        let last_stable_mci = storage::read_last_stable_mc_index(db)?;
        let last_known_mci = storage::read_last_main_chain_index(db)?;
        let witnesses: &[String] = &::my_witness::MY_WITNESSES;
        let param = json!({
                "witnesses": witnesses,
                "last_stable_mci": last_stable_mci,
                "last_known_mci": last_known_mci
            });

        let ret = self.send_request("catchup", &param).unwrap();
        if !ret["error"].is_null() {
            bail!("catchup request got error response: {:?}", ret["error"]);
        }

        let catchup_chain: catchup::CatchupChain = serde_json::from_value(ret).unwrap();

        // print out unsupported messages!
        for j in &catchup_chain.stable_last_ball_joints {
            for m in &j.unit.messages {
                if let Some(::spec::Payload::Other(v)) = &m.payload {
                    error!("app = {}, v = {}", m.app, v);
                }
            }
        }

        catchup::process_catchup_chain(&db, catchup_chain)?;

        Ok(())
    }

    fn request_new_missing_joints(&self, db: &Connection, units: &[String]) -> Result<()> {
        let mut new_units = Vec::new();

        for unit in units {
            let g = UNIT_IN_WORK.try_lock(vec![unit.clone()]);
            if g.is_none() {
                // other thread is working on the unit, skip it
                continue;
            }

            use joint_storage::CheckNewResult;
            match joint_storage::check_new_unit(db, unit)? {
                CheckNewResult::New => {
                    new_units.push(unit.clone());
                }
                CheckNewResult::Known => info!("unit {} is already known", unit),
                CheckNewResult::KnownUnverified => info!("unit {} known unverified", unit),
                CheckNewResult::KnownBad => error!("unit {} known bad, ignore it", unit),
            }
        }

        if !new_units.is_empty() {
            self.request_joints(&new_units)?;
        }
        Ok(())
    }

    fn request_next_hash_tree(
        &self,
        db: &mut Connection,
        from_ball: &str,
        to_ball: &str,
    ) -> Result<()> {
        // TODO: need reroute if failed to send
        let mut hash_tree = self.send_request(
            "get_hash_tree",
            &json!({
                "from_ball": from_ball,
                "to_ball": to_ball,
            }),
        )?;

        if !hash_tree["error"].is_null() {
            error!("get_hash_tree got error response: {}", hash_tree["error"]);
            return Ok(());
        }

        let balls: Vec<catchup::BallProps> = serde_json::from_value(hash_tree["balls"].take())?;
        let units: Vec<String> = balls.iter().map(|b| b.unit.clone()).collect();
        catchup::process_hash_tree(db, balls)?;
        self.request_new_missing_joints(db, &units)?;
        Ok(())
    }

    #[allow(dead_code)]
    fn notify_watchers(&self, joint: &Joint) -> Result<()> {
        let _ = joint;
        unimplemented!()
    }

    #[inline]
    fn send_joint(&self, joint: &Joint) -> Result<()> {
        self.send_just_saying("joint", serde_json::to_value(joint)?)
    }

    fn send_joints_since_mci(&self, db: &Connection, mci: u32) -> Result<()> {
        let joints = joint_storage::read_joints_since_mci(db, mci)?;

        for joint in joints {
            self.send_joint(&joint)?;
        }
        self.send_just_saying("free_joints_end", Value::Null)?;

        Ok(())
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
        let challenge = object_hash::get_random_string(30);
        self.send_just_saying("hub/challenge", json!(challenge))?;
        Ok(())
    }

    fn send_subscribe(&self) -> Result<()> {
        use object_hash;
        // TODO: this is used to detect self-connect (#63)
        let subscription_id = object_hash::get_random_string(30);
        let db = ::db::DB_POOL.get_connection();
        let last_mci = storage::read_last_main_chain_index(&db)?;
        self.send_request(
            "subscribe",
            &json!({ "subscription_id": subscription_id, "last_mci": last_mci}),
        )?;

        self.set_source();
        Ok(())
    }

    fn send_heartbeat(&self) -> Result<()> {
        self.send_request("heartbeat", &Value::Null)?;
        Ok(())
    }

    // remove self from global
    fn close(&self) {
        info!("close connection: {}", self.get_peer());
        WSS.close(self);
    }

    fn request_joints(&self, units: &[String]) -> Result<()> {
        fn request_joint(ws: Arc<HubConn>, unit: &str) -> Result<()> {
            // if the joint is in request, just ignore
            let g = JOINT_IN_REQ.try_lock(vec![unit.to_owned()]);
            if g.is_none() {
                println!(
                    "\n\nrequest_joint lock failed!!!!!!!!!!!!!!!!!: {}\n\n",
                    unit
                );
                return Ok(());
            }

            let mut v = ws.send_request("get_joint", &json!(unit))?;
            if v["joint_not_found"].as_str() == Some(&unit) {
                // TODO: if self connection failed to request jonit, should
                // let available ws to try a again here. see #72
                bail!(
                    "unit {} not found with the connection: {}",
                    unit,
                    ws.get_peer()
                );
            }

            let joint: Joint = serde_json::from_value(v["joint"].take())?;
            info!("receive a requested joint: {:?}", joint);
            match &joint.unit.unit {
                None => bail!("no unit"),
                Some(unit_hash) => {
                    if unit_hash != unit {
                        let err = format!("I didn't request this unit from you: {}", unit_hash);
                        return ws.send_error(json!(err));
                    }
                }
            }

            ws.handle_online_joint(joint, &mut db::DB_POOL.get_connection())
        }

        for unit in units {
            let unit = unit.clone();
            let ws = WSS.get_ws(self);
            try_go!(move || request_joint(ws, &unit));
        }
        Ok(())
    }
}

pub fn create_outbound_conn<A: ToSocketAddrs>(address: A) -> Result<Arc<HubConn>> {
    let stream = TcpStream::connect(address)?;
    let peer = match stream.peer_addr() {
        Ok(addr) => addr.to_string(),
        Err(_) => "unknown peer".to_owned(),
    };
    let url = Url::parse("wss://localhost/")?;
    let req = Request::from(url);
    let (conn, _) = client(req, stream)?;
    // let ws
    let ws = WsConnection::new(conn, HubData::default(), peer, Role::Client)?;

    WSS.add_outbound(ws.clone());
    Ok(ws)
}

fn check_catchup_leftover(db: &Connection) -> Result<bool> {
    catchup::purge_handled_balls_from_hash_tree(db)?;

    // check leftover 1
    let mut stmt = db.prepare_cached(
        "SELECT hash_tree_balls.unit FROM hash_tree_balls LEFT JOIN units USING(unit) \
         WHERE units.unit IS NULL ORDER BY ball_index",
    )?;
    if stmt.exists(&[])? {
        return Ok(true);
    }

    // check leftover 2
    let mut stmt = db.prepare_cached("SELECT 1 FROM catchup_chain_balls LIMIT 1")?;
    if stmt.exists(&[])? {
        return Ok(true);
    }

    Ok(false)
}

pub fn purge_junk_unhandled_joints(db: &Connection) -> Result<()> {
    let diff = ::time::now() - COMING_ONLINE_TIME.load(Ordering::Relaxed) as u64;
    if diff < 3600 * 1000 || IS_CACTCHING_UP.is_locked() {
        return Ok(());
    }

    let mut stmt = db.prepare_cached(
        "DELETE FROM unhandled_joints WHERE creation_date < datatime('now', '-1 HOUR')",
    )?;
    stmt.execute(&[])?;

    let mut stmt = db.prepare_cached(
        "DELETE FROM dependencies WHERE NOT EXISTS \
         (SELECT * FROM unhandled_joints WHERE unhandled_joints.unit=dependencies.unit)",
    )?;
    stmt.execute(&[])?;
    Ok(())
}

// this is a back ground thread that focuse on the catchup logic
pub fn start_catchup() -> Result<()> {
    // if we already in catchup mode, just return
    let _g = match IS_CACTCHING_UP.try_lock() {
        Some(g) => g,
        None => return Ok(()),
    };

    let mut ws = match WSS.get_next_outbound() {
        None => bail!("no outbound connection found"),
        Some(c) => c,
    };

    let mut db = db::DB_POOL.get_connection();
    catchup::purge_handled_balls_from_hash_tree(&db)?;

    let mut is_left_over = check_catchup_leftover(&db)?;
    if !is_left_over {
        ws.request_catchup(&db)?;
    }

    loop {
        // if there is no more work, start a new batch
        let balls = {
            let mut stmt = db.prepare_cached(
                "SELECT 1 FROM hash_tree_balls LEFT JOIN units USING(unit) \
                 WHERE units.unit IS NULL LIMIT 1",
            )?;
            is_left_over = stmt.exists(&[])?;

            // try to start a new batch
            let mut stmt = db.prepare_cached(
                "SELECT ball FROM catchup_chain_balls ORDER BY member_index LIMIT 2",
            )?;
            let mut balls = stmt
                .query_map(&[], |row| row.get(0))?
                .collect::<::std::result::Result<Vec<String>, _>>()?;

            if balls.len() == 1 {
                let mut stmt = db.prepare_cached("DELETE FROM catchup_chain_balls WHERE ball=?")?;
                stmt.execute(&[&balls[0]])?;
                balls.clear();
            }

            balls
        };

        if balls.is_empty() {
            if is_left_over {
                // every one second check again
                info!("wait for catchup data consumed!");
                coroutine::sleep(Duration::from_secs(1));
                continue;
            } else {
                // we have done
                info!("catchup done!");
                break;
            }
        }

        if let Err(e) = ws.request_next_hash_tree(&mut db, &balls[0], &balls[1]) {
            error!("request_next_hash_tree err={}", e);
            ws.close();
            // we try with a different connection
            ws = match WSS.get_next_outbound() {
                None => bail!("can't find outbound connection"),
                Some(c) => c,
            };
        }
    }

    // now we are done the catchup
    COMING_ONLINE_TIME.store(::time::now() as usize, Ordering::Relaxed);
    WSS.request_free_joints();

    Ok(())
}

/// this fn will be called every 8s in a timer
pub fn re_requeset_lost_joints(db: &Connection) -> Result<()> {
    let _g = match IS_CACTCHING_UP.try_lock() {
        Some(g) => g,
        None => return Ok(()),
    };

    let ws = match WSS.get_next_peer() {
        None => bail!("failed to find next peer"),
        Some(c) => c,
    };

    let units = joint_storage::find_lost_joints(db)?;
    info!("lost units {:?}", units);

    info!("found next peer {}", ws.get_peer());

    // this is not an atomic operation, but it's fine to request the unit in working
    let new_units = units
        .iter()
        .filter(|x| UNIT_IN_WORK.try_lock(vec![(*x).to_owned()]).is_none())
        .cloned()
        .collect::<Vec<_>>();

    ws.request_joints(&new_units)
}

pub fn find_and_handle_joints_that_are_ready(
    db: &mut Connection,
    unit: Option<&String>,
) -> Result<()> {
    lazy_static! {
        static ref DEPENDENCIES: Mutex<()> = Mutex::new(());
    }
    let _g = DEPENDENCIES.lock().unwrap();
    let mut unhandled_joints = VecDeque::new();

    let joints = joint_storage::read_dependent_joints_that_are_ready(db, unit)?;

    for joint in joints {
        unhandled_joints.push_back(joint);
    }

    while let Some(joint) = unhandled_joints.pop_front() {
        let ReadyJoint {
            joint,
            create_ts,
            peer,
        } = joint;

        let ws = match WSS.get_connection_by_name(&peer) {
            Some(c) => c,
            None => match WSS.get_next_peer() {
                Some(c) => c,
                None => bail!("no connection for find_and_handle_joints_that_are_ready"),
            },
        };
        // this is not safe to run in multi-thread, not in parallel
        ws.handle_saved_joint(db, joint, create_ts, &mut unhandled_joints)?;
    }

    // TODO:
    // self.handle_saved_private_payment()?;
    Ok(())
}
