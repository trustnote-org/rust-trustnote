use std::collections::VecDeque;
use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::network::{Sender, Server, WsConnection};
use catchup;
use config;
use crossbeam::atomic::ArcCell;
use db;
use error::Result;
use failure::ResultExt;
use joint::Joint;
use joint_storage::{self, ReadyJoint};
use light::{self, HistoryRequest, HistoryResponse};
use may::coroutine;
use may::net::TcpStream;
use may::sync::{Mutex, RwLock};
use object_hash;
use rusqlite::Connection;
use serde_json::{self, Value};
use signature;
use storage;
use tungstenite::client::client;
use tungstenite::handshake::client::Request;
use tungstenite::protocol::Role;
use url::Url;
use utils::{AtomicLock, MapLock};
use validation;

#[derive(Serialize, Deserialize)]
pub struct Login {
    pub challenge: String,
    pub pubkey: String,
    #[serde(skip_serializing)]
    pub signature: String,
}

#[derive(Serialize, Deserialize)]
pub struct TempPubkey {
    pub pubkey: String,
    pub temp_pubkey: String,
    #[serde(skip_serializing)]
    pub signature: String,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum DeviceMessage {
    Login(Login),
    TempPubkey(TempPubkey),
}

impl DeviceMessage {
    // prefix device addresses with 0 to avoid confusion with payment addresses
    // Note that 0 is not a member of base32 alphabet, which makes device addresses easily distinguishable from payment addresses
    // but still selectable by double-click.  Stripping the leading 0 will not produce a payment address that the device owner knows a private key for,
    // because payment address is derived by c-hashing the definition object, while device address is produced from raw public key.
    fn get_device_address(&self) -> Result<String> {
        let mut address = match *self {
            DeviceMessage::Login(ref login) => object_hash::get_chash(&login.pubkey)?,
            DeviceMessage::TempPubkey(ref temp_pubkey) => {
                object_hash::get_chash(&temp_pubkey.pubkey)?
            }
        };

        address.insert(0, '0');
        Ok(address)
    }

    fn get_device_message_hash_to_sign(&self) -> Vec<u8> {
        use sha2::{Digest, Sha256};

        let source_string = ::obj_ser::to_string(self).expect("DeviceMessage to string failed");
        Sha256::digest(source_string.as_bytes()).to_vec()
    }
}

pub struct HubData {
    // indicate if this connection is a subscribed peer
    is_subscribed: AtomicBool,
    is_source: AtomicBool,
    is_inbound: AtomicBool,
    is_login_completed: AtomicBool,
    challenge: ArcCell<Option<String>>,
    device_address: ArcCell<Option<String>>,
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
    static ref SUBSCRIPTION_ID: RwLock<String> = RwLock::new(object_hash::gen_random_string(30));
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
        self.inbound.write().unwrap().push(inbound.clone());
        inbound.set_inbound();
        init_connection(&inbound);
        t!(add_peer_host(inbound));
    }

    pub fn add_outbound(&self, outbound: Arc<HubConn>) {
        self.outbound.write().unwrap().push(outbound.clone());
        init_connection(&outbound);
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

    fn get_peers_from_remote(&self) -> Result<Vec<String>> {
        let mut peers: Vec<String> = Vec::new();

        let out_bound_peers = self.outbound.read().unwrap().to_vec();
        for out_bound_peer in out_bound_peers {
            let mut tmp: Vec<String> =
                serde_json::from_value(out_bound_peer.send_request("get_peers", &Value::Null)?)?;
            peers.append(&mut tmp);
        }

        let in_bound_peers = self.inbound.read().unwrap().to_vec();
        for in_bound_peer in in_bound_peers {
            let mut tmp: Vec<String> =
                serde_json::from_value(in_bound_peer.send_request("get_peers", &Value::Null)?)?;
            peers.append(&mut tmp);
        }

        Ok(peers)
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

    fn forward_joint(&self, cur_ws: &HubConn, joint: &Joint) -> Result<()> {
        for c in &*self.outbound.read().unwrap() {
            if c.is_subscribed() && !c.conn_eq(cur_ws) {
                c.send_joint(joint)?;
            }
        }

        for c in &*self.inbound.read().unwrap() {
            if c.is_subscribed() && !c.conn_eq(cur_ws) {
                c.send_joint(joint)?;
            }
        }
        Ok(())
    }

    pub fn request_free_joints_from_all_outbound_peers(&self) -> Result<()> {
        let out_bound_peers = self.outbound.read().unwrap().to_vec();
        for out_bound_peer in out_bound_peers {
            out_bound_peer.send_just_saying("refresh", Value::Null)?;
        }
        Ok(())
    }

    pub fn get_outbound_peers(&self) -> Vec<String> {
        self.outbound
            .read()
            .unwrap()
            .iter()
            .map(|c| c.get_peer().to_owned())
            .collect()
    }

    pub fn get_inbound_peers(&self) -> Vec<String> {
        self.inbound
            .read()
            .unwrap()
            .iter()
            .map(|c| c.get_peer().to_owned())
            .collect()
    }

    fn get_needed_outbound_peers(&self) -> usize {
        let outbound_connecions = self.outbound.read().unwrap().len();
        if config::MAX_OUTBOUND_CONNECTIONS > outbound_connecions {
            return config::MAX_OUTBOUND_CONNECTIONS - outbound_connecions;
        }
        return 0;
    }

    fn contains(&self, addr: &str) -> bool {
        let out_contains = self
            .outbound
            .read()
            .unwrap()
            .iter()
            .any(|v| v.get_peer() == addr);
        let in_contains = self
            .inbound
            .read()
            .unwrap()
            .iter()
            .any(|v| v.get_peer() == addr);
        out_contains || in_contains
    }
}

fn get_unconnected_peers_in_config() -> Result<Vec<String>> {
    let config_peers = config::get_remote_hub_url();
    Ok(config_peers
        .into_iter()
        .filter(|peer| !WSS.contains(peer))
        .collect::<Vec<_>>())
}

fn get_unconnected_peers_in_db() -> Result<Vec<String>> {
    let max_new_outbound_peers = WSS.get_needed_outbound_peers();

    let sql_out = WSS
        .get_outbound_peers()
        .into_iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");

    let sql_in = WSS
        .get_inbound_peers()
        .into_iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");

    let db = db::DB_POOL.get_connection();
    let sql = format!(
        "SELECT peer FROM peers \
         JOIN peer_hosts USING(peer_host) \
         LEFT JOIN peer_host_urls ON peer=url AND is_active=1 \
         WHERE (count_invalid_joints/count_new_good_joints< 0.2 \
         OR count_new_good_joints=0 AND count_nonserial_joints=0 AND count_invalid_joints=0) \
         AND peer NOT IN({})  AND (peer_host_urls.peer_host IS NULL OR \
         peer_host_urls.peer_host NOT IN({})) AND is_self=0 \
         ORDER BY random() LIMIT ?",
        sql_out, sql_in
    );

    let mut stmt = db.prepare_cached(&sql)?;
    let peers = stmt
        .query_map(&[&(max_new_outbound_peers as u32)], |v| v.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;
    Ok(peers)
}

pub fn get_unconnected_remote_peers() -> Result<Vec<String>> {
    let peers = WSS.get_peers_from_remote()?;

    Ok(peers
        .into_iter()
        .filter(|peer| !WSS.contains(peer))
        .collect::<Vec<_>>())
}

pub fn auto_connection() -> Result<()> {
    let mut counts = WSS.get_needed_outbound_peers();
    if counts == 0 {
        return Ok(());
    }

    if let Ok(peers) = get_unconnected_peers_in_config() {
        for peer in peers {
            if create_outbound_conn(peer).is_ok() {
                counts -= 1;
                if counts == 0 {
                    return Ok(());
                }
            }
        }
    }

    if let Ok(peers) = get_unconnected_remote_peers() {
        for peer in peers {
            if create_outbound_conn(peer).is_ok() {
                counts -= 1;
                if counts == 0 {
                    return Ok(());
                }
            }
        }
    }

    if let Ok(peers) = get_unconnected_peers_in_db() {
        for peer in peers {
            if create_outbound_conn(peer).is_ok() {
                counts -= 1;
                if counts == 0 {
                    return Ok(());
                }
            }
        }
    }

    Ok(())
}

impl Default for HubData {
    fn default() -> Self {
        HubData {
            is_subscribed: AtomicBool::new(false),
            is_source: AtomicBool::new(false),
            is_inbound: AtomicBool::new(false),
            is_login_completed: AtomicBool::new(false),
            challenge: ArcCell::new(Arc::new(None)),
            device_address: ArcCell::new(Arc::new(None)),
        }
    }
}

impl Server<HubData> for HubData {
    fn on_message(ws: Arc<HubConn>, subject: String, body: Value) -> Result<()> {
        match subject.as_str() {
            "version" => ws.on_version(body)?,
            "hub/challenge" => ws.on_hub_challenge(body)?,
            "free_joints_end" => {} // not handled
            "error" => error!("receive error: {}", body),
            "info" => info!("receive info: {}", body),
            "result" => info!("receive result: {}", body),
            "joint" => ws.on_joint(body)?,
            "refresh" => ws.on_refresh(body)?,
            "light/new_address_to_watch" => ws.on_new_address_to_watch(body)?,
            "hub/login" => ws.on_hub_login(body)?,
            subject => bail!(
                "on_message unknown subject: {} body {}",
                subject,
                body.to_string()
            ),
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
            // bellow is wallet used command
            "get_bots" => ws.on_get_bots(params)?,
            "hub/temp_pubkey" => ws.on_hub_temp_pubkey(params)?,
            "get_peers" => ws.on_get_peers(params)?,
            "get_witnesses" => ws.on_get_witnesses(params)?,
            "post_joint" => ws.on_post_joint(params)?,
            "light/get_history" => ws.on_get_history(params)?,
            "light/get_link_proofs" => ws.on_get_link_proofs(params)?,
            "light/get_parents_and_last_ball_and_witness_list_unit" => {
                ws.on_get_parents_and_last_ball_and_witness_list_unit(params)?
            }
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

    pub fn is_inbound(&self) -> bool {
        let data = self.get_data();
        data.is_inbound.load(Ordering::Relaxed)
    }

    pub fn set_inbound(&self) {
        let data = self.get_data();
        data.is_inbound.store(true, Ordering::Relaxed);
    }

    pub fn is_login_completed(&self) -> bool {
        let data = self.get_data();
        data.is_login_completed.load(Ordering::Relaxed)
    }

    pub fn set_login_completed(&self) {
        let data = self.get_data();
        data.is_login_completed.store(true, Ordering::Relaxed);
    }

    pub fn get_challenge(&self) -> Arc<Option<String>> {
        let data = self.get_data();
        data.challenge.get()
    }

    pub fn set_challenge(&self, challenge: &str) {
        let data = self.get_data();
        data.challenge.set(Arc::new(Some(challenge.to_owned())));
    }

    pub fn get_device_address(&self) -> Arc<Option<String>> {
        let data = self.get_data();
        data.device_address.get()
    }

    pub fn set_device_address(&self, device_address: &str) {
        info!("set_device_address {}", device_address);
        let data = self.get_data();
        data.device_address
            .set(Arc::new(Some(device_address.to_owned())));
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
        let subscription_id = param["subscription_id"]
            .as_str()
            .ok_or_else(|| format_err!("no subscription_id"))?;
        if subscription_id == *SUBSCRIPTION_ID.read().unwrap() {
            let db = db::DB_POOL.get_connection();
            let mut stmt = db.prepare_cached("UPDATE peers SET is_self=1 WHERE peer=?")?;
            stmt.execute(&[self.get_peer()])?;

            self.close();
            return Err(format_err!("self-connect"));
        }
        self.set_subscribed();
        // send some joint in a background task
        let ws = WSS.get_ws(self);
        let last_mci = param["last_mci"].as_u64();
        try_go!(move || -> Result<()> {
            let db = db::DB_POOL.get_connection();
            if let Some(last_mci) = last_mci {
                ws.send_joints_since_mci(&db, last_mci as u32)?;
            } else {
                ws.send_free_joints(&db)?;
            }
            Ok(())
        });

        Ok(Value::from("subscribed"))
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
            Ok(joint) => Ok(json!({ "joint": joint })),
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

            if stmt.exists(&[joint.get_unit_hash()])? {
                return self.send_error(Value::from("this unit is already known and archived"));
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

    fn on_refresh(&self, param: Value) -> Result<()> {
        let _g = match IS_CACTCHING_UP.try_lock() {
            Some(g) => g,
            None => return Ok(()),
        };

        let mci = param.as_u64();
        let db = db::DB_POOL.get_connection();
        if let Some(mci) = mci {
            self.send_joints_since_mci(&db, mci as u32)?;
        } else {
            self.send_free_joints(&db)?;
        }

        Ok(())
    }

    fn on_new_address_to_watch(&self, param: Value) -> Result<()> {
        if !self.is_inbound() {
            return self.send_error(Value::from("light clients have to be inbound"));
        }

        let address: String = serde_json::from_value(param).context("not an address string")?;
        if !::object_hash::is_chash_valid(&address) {
            return self.send_error(Value::from("address not valid"));
        }

        let db = db::DB_POOL.get_connection();
        let mut stmt = db.prepare_cached(
            "INSERT OR IGNORE INTO watched_light_addresses (peer, address) VALUES (?,?)",
        )?;
        stmt.execute(&[self.get_peer(), &address])?;
        self.send_info(Value::from(format!("now watching {}", address)))?;

        let mut stmt = db.prepare_cached(
            "SELECT unit, is_stable FROM unit_authors JOIN units USING(unit) WHERE address=? \
             UNION \
             SELECT unit, is_stable FROM outputs JOIN units USING(unit) WHERE address=? \
             ORDER BY is_stable LIMIT 10",
        )?;

        struct TempUnit {
            unit: String,
            is_stable: u32,
        }

        let rows = stmt
            .query_map(&[&address, &address], |row| TempUnit {
                unit: row.get(0),
                is_stable: row.get(1),
            })?.collect::<::std::result::Result<Vec<_>, _>>()?;

        if rows.is_empty() {
            return Ok(());
        }

        if rows.len() == 10 || rows.iter().any(|r| r.is_stable == 1) {
            self.send_just_saying("light/have_updates", Value::Null)?;
        }

        for row in rows {
            if row.is_stable == 1 {
                continue;
            }
            let joint = storage::read_joint(&db, &row.unit)
                .context(format!("watched unit {} not found", row.unit))?;
            self.send_joint(&joint)?;
        }

        Ok(())
    }

    fn on_get_bots(&self, _param: Value) -> Result<Value> {
        let db = db::DB_POOL.get_connection();
        let mut stmt = db.prepare_cached(
            "SELECT id, name, pairing_cod, description FROM bots ORDER BY rank DESC, id",
        )?;

        #[derive(Serialize)]
        struct Bot {
            id: u32,
            name: String,
            pairing_cod: String,
            description: String,
        };

        let bots = stmt
            .query_map(&[], |row| Bot {
                id: row.get(0),
                name: row.get(1),
                pairing_cod: row.get(2),
                description: row.get(3),
            })?.collect::<::std::result::Result<Vec<_>, _>>()?;
        Ok(serde_json::to_value(bots)?)
    }

    fn on_get_peers(&self, _param: Value) -> Result<Value> {
        let peers = WSS.get_outbound_peers();
        Ok(serde_json::to_value(peers)?)
    }

    fn on_get_witnesses(&self, _: Value) -> Result<Value> {
        use my_witness::MY_WITNESSES;
        Ok(serde_json::to_value(&*MY_WITNESSES)?)
    }

    fn on_post_joint(&self, param: Value) -> Result<Value> {
        let joint: Joint = serde_json::from_value(param)?;
        info!("receive a posted joint: {:?}", joint);
        ensure!(joint.unit.unit.is_some(), "no unit");
        let mut db = db::DB_POOL.get_connection();
        self.handle_posted_joint(joint, &mut db)?;
        Ok(Value::from("accepted"))
    }

    fn on_get_history(&self, param: Value) -> Result<Value> {
        if !self.is_inbound() {
            bail!("light clients have to be inbound");
        }

        let history_request: HistoryRequest = serde_json::from_value(param)?;

        let ret = self.handle_get_history(history_request)?;

        Ok(serde_json::to_value(ret)?)
    }

    fn on_get_link_proofs(&self, params: Value) -> Result<Value> {
        if !self.is_inbound() {
            bail!("light clients have to be inbound");
        }
        let units: Vec<String> =
            serde_json::from_value(params).context("prepare_Link_proofs.params is error")?;
        Ok(serde_json::to_value(light::prepare_link_proofs(&units)?)?)
    }

    fn on_get_parents_and_last_ball_and_witness_list_unit(&self, param: Value) -> Result<Value> {
        if !self.is_inbound() {
            bail!("light clients have to be inbound");
        }

        #[derive(Deserialize)]
        struct TempWitnesses {
            witnesses: Vec<String>,
        }

        let witnesses: TempWitnesses = serde_json::from_value(param).context("no witnesses")?;

        let ret = light::prepare_parents_and_last_ball_and_witness_list_unit(&witnesses.witnesses)
            .context("failed to get parents_and_last_ball_and_witness_list_unit")?;

        Ok(serde_json::to_value(ret)?)
    }

    fn on_hub_login(&self, body: Value) -> Result<()> {
        match serde_json::from_value::<DeviceMessage>(body) {
            Err(e) => {
                error!("hub_login: serde err= {}", e);
                return self.send_error(Value::from("no login params"));
            }
            Ok(device_message) => {
                if let DeviceMessage::Login(ref login) = &device_message {
                    if Some(&login.challenge) != (*self.get_challenge()).as_ref() {
                        return self.send_error(Value::from("wrong challenge"));
                    }

                    if login.pubkey.len() != ::config::PUBKEY_LENGTH {
                        return self.send_error(Value::from("wrong pubkey length"));
                    }

                    if login.signature.len() != ::config::SIG_LENGTH {
                        return self.send_error(Value::from("wrong signature length"));
                    };

                    if signature::verify(
                        &device_message.get_device_message_hash_to_sign(),
                        &login.signature,
                        &login.pubkey,
                    ).is_err()
                    {
                        return self.send_error(Value::from("wrong signature"));
                    }

                    let device_address = device_message.get_device_address()?;
                    self.set_device_address(&device_address);

                    self.send_just_saying("hub/push_project_number", json!({"projectNumber": 0}))?;

                    // after this point the device is authenticated and can send further commands
                    let db = db::DB_POOL.get_connection();
                    let mut stmt =
                        db.prepare_cached("SELECT 1 FROM devices WHERE device_address=?")?;
                    if !stmt.exists(&[&device_address])? {
                        let mut stmt = db.prepare_cached(
                            "INSERT INTO devices (device_address, pubkey) VALUES (?,?)",
                        )?;
                        stmt.execute(&[&device_address, &login.pubkey])?;
                        self.send_info(json!("address created"))?;
                    } else {
                        self.send_stored_device_messages(&db, &device_address)?;
                    }

                    //finishLogin
                    self.set_login_completed();
                //TODO: Seems to handle the temp_pubkey message before the login happen
                } else {
                    return self.send_error(Value::from("not a valid login DeviceMessage"));
                }
            }
        }

        Ok(())
    }

    fn on_hub_temp_pubkey(&self, param: Value) -> Result<Value> {
        let mut try_limit = 20;
        while try_limit > 0 && self.get_device_address().is_none() {
            try_limit -= 1;
            coroutine::sleep(Duration::from_millis(100));
        }

        let device_address = self.get_device_address();

        // ensure!(device_address.is_some(), "please log in first");

        match serde_json::from_value::<DeviceMessage>(param) {
            Err(e) => {
                error!("temp_pubkey serde err={}", e);
                bail!("wrong temp_pubkey params");
            }

            Ok(device_message) => {
                if let DeviceMessage::TempPubkey(ref temp_pubkey) = &device_message {
                    ensure!(
                        temp_pubkey.temp_pubkey.len() == ::config::PUBKEY_LENGTH,
                        "wrong temp_pubkey length"
                    );
                    ensure!(
                        Some(device_message.get_device_address()?) == *self.get_device_address(),
                        "signed by another pubkey"
                    );

                    if signature::verify(
                        &device_message.get_device_message_hash_to_sign(),
                        &temp_pubkey.signature,
                        &temp_pubkey.pubkey,
                    ).is_err()
                    {
                        bail!("wrong signature");
                    }

                    let db = db::DB_POOL.get_connection();
                    let mut stmt = db.prepare_cached(
                        "UPDATE devices SET temp_pubkey_package=? WHERE device_address=?",
                    )?;
                    // TODO: here need to add signature back
                    stmt.execute(&[&serde_json::to_string(temp_pubkey)?, &*device_address])?;

                    return Ok(Value::from("updated"));
                } else {
                    bail!("not a valid temp_pubkey params");
                }
            }
        }
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
                    joint.save(validate_state, false)?;
                    drop(lock);

                    self.send_result(json!({"unit": unit, "result": "accepted"}))?;

                    if !IS_CACTCHING_UP.is_locked() {
                        WSS.forward_joint(self, &joint)?;
                    }
                    notify_watchers(db, &joint, self)?;

                    // must release the guard to let other work continue
                    drop(g);

                    // wake up other joints that depend on me
                    find_and_handle_joints_that_are_ready(db, Some(unit))?;
                }
            },
            Err(err) => match err {
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
                    let mut stmt = db.prepare_cached(
                        "INSERT INTO known_bad_joints (joint, json, error) VALUES (?,?,?)",
                    )?;
                    stmt.execute(&[
                        &object_hash::get_base64_hash(&joint)?,
                        &serde_json::to_string(&joint)?,
                        &err,
                    ])?;
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
            },
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
                    joint.save(validation_state, false)?;
                    drop(lock);

                    self.send_result(json!({"unit": unit, "result": "accepted"}))?;

                    const FORWARDING_TIMEOUT: u64 = 10 * 1000;
                    if !IS_CACTCHING_UP.is_locked()
                        && create_ts as u64 > ::time::now() - FORWARDING_TIMEOUT
                    {
                        WSS.forward_joint(self, &joint)?;
                    }
                    notify_watchers(db, &joint, self)?;
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
            Err(err) => match err {
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
                    let mut stmt = db.prepare_cached(
                        "INSERT INTO known_bad_joints (joint, json, error) VALUES (?,?,?)",
                    )?;
                    stmt.execute(&[
                        &object_hash::get_base64_hash(&joint)?,
                        &serde_json::to_string(&joint)?,
                        &err,
                    ])?;
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
            },
        }

        Ok(())
    }

    fn handle_posted_joint(&self, mut joint: Joint, db: &mut Connection) -> Result<()> {
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
                self.write_event(db, "know_good")?;
                bail!("known");
            }
            CheckNewResult::KnownBad => {
                self.write_event(db, "know_bad")?;
                bail!("known bad");
            }

            CheckNewResult::KnownUnverified => {
                bail!("known unverified");
            }
        }

        match validation::validate(db, &joint) {
            Ok(ok) => match ok {
                ValidationOk::Unsigned(_) => {
                    if joint.unsigned != Some(true) {
                        bail!("ifOkUnsigned() signed");
                    }
                    bail!("you can't send unsigned units");
                }
                ValidationOk::Signed(validate_state, lock) => {
                    if joint.unsigned == Some(true) {
                        bail!("ifOk() unsigned");
                    }
                    joint.save(validate_state, false)?;
                    drop(lock);

                    if !IS_CACTCHING_UP.is_locked() {
                        WSS.forward_joint(self, &joint)?;
                    }
                    notify_watchers(db, &joint, self)?;
                }
            },
            Err(err) => match err {
                ValidationError::OtherError { err } => {
                    bail!("validation other err={}, unit={}", err, unit);
                }

                ValidationError::UnitError { err } => {
                    self.purge_joint_and_dependencies_and_notify_peers(db, &joint, &err)?;
                    if !err.contains("authentifier verification failed")
                        && !err.contains("bad merkle proof at path")
                    {
                        self.write_event(db, "invalid")?;
                    }
                    bail!("{} validation failed: {}", unit, err);
                }
                ValidationError::JointError { err } => {
                    self.write_event(db, "invalid")?;
                    let mut stmt = db.prepare_cached(
                        "INSERT INTO known_bad_joints (joint, json, error) VALUES (?,?,?)",
                    )?;
                    stmt.execute(&[
                        &object_hash::get_base64_hash(&joint)?,
                        &serde_json::to_string(&joint)?,
                        &err,
                    ])?;
                    bail!("{}", err);
                }
                ValidationError::NeedHashTree => {
                    info!("need hash tree for unit {}", unit);
                    if joint.unsigned == Some(true) {
                        bail!("need hash tree unsigned");
                    }
                    bail!("need hash tree");
                }
                ValidationError::NeedParentUnits(_) => bail!("unknown parents"),
            },
        }

        Ok(())
    }

    fn handle_get_history(&self, history_request: HistoryRequest) -> Result<HistoryResponse> {
        let db = db::DB_POOL.get_connection();
        let ret = light::prepare_history(&db, &history_request)?;

        let params_addresses = history_request.addresses;
        if !params_addresses.is_empty() {
            let addresses = params_addresses
                .iter()
                .map(|s| format!("('{}','{}')", self.get_peer(), s))
                .collect::<Vec<_>>()
                .join(", ");

            let sql = format!(
                "INSERT OR IGNORE INTO watched_light_addresses (peer, address) VALUES {}",
                addresses
            );
            let mut stmt = db.prepare(&sql)?;
            stmt.execute(&[])?;
        }

        let params_requested_joints = history_request.requested_joints;
        if !params_requested_joints.is_empty() {
            let rows = storage::slice_and_execute_query(
                &db,
                "SELECT unit FROM units WHERE main_chain_index >= ? AND unit IN({})",
                &[&storage::get_min_retrievable_mci()],
                &params_requested_joints,
                |row| row.get(0),
            )?;
            if !rows.is_empty() {
                let rows = rows
                    .into_iter()
                    .map(|s: String| format!("('{}','{}')", self.get_peer(), s))
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "INSERT OR IGNORE INTO watched_light_addresses (peer, address) VALUES {}",
                    rows
                );
                let mut stmt = db.prepare(&sql)?;
                stmt.execute(&[])?;
            }
        }
        Ok(ret)
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
        // for j in &catchup_chain.stable_last_ball_joints {
        //     for m in &j.unit.messages {
        //         if let Some(::spec::Payload::Other(v)) = &m.payload {
        //             error!("app = {}, v = {}", m.app, v);
        //         }
        //     }
        // }

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

    fn send_free_joints(&self, db: &Connection) -> Result<()> {
        let joints = storage::read_free_joints(db).context("send free joint failed")?;
        for joint in joints {
            self.send_joint(&joint)?;
        }
        self.send_just_saying("free_joints_end", Value::Null)?;
        Ok(())
    }

    fn send_stored_device_messages(
        &self,
        _db: &Connection,
        _device_address: &String,
    ) -> Result<()> {
        //TODO: save and send device messages
        Ok(())
    }
}

// the client side impl
impl HubConn {
    fn send_version(&self) -> Result<()> {
        self.send_just_saying(
            "version",
            json!({
                "protocol_version": config::VERSION,
                "alt": config::ALT,
                "library": config::LIBRARY,
                "library_version": config::LIBRARY_VERSION,
                "program": "rust-trustnote-hub",
                // TODO: read from Cargo.toml
                "program_version": "0.1.0"
            }),
        )
    }

    fn send_hub_challenge(&self) -> Result<()> {
        let challenge = object_hash::gen_random_string(30);
        self.set_challenge(&challenge);
        self.send_just_saying("hub/challenge", Value::from(challenge))?;
        Ok(())
    }

    fn send_subscribe(&self) -> Result<()> {
        let db = ::db::DB_POOL.get_connection();
        let last_mci = storage::read_last_main_chain_index(&db)?;
        self.send_request(
            "subscribe",
            &json!({ "subscription_id": *SUBSCRIPTION_ID.read().unwrap(), "last_mci": last_mci}),
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

            let mut v = ws.send_request("get_joint", &Value::from(unit))?;
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
                        return ws.send_error(Value::from(err));
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
    error!("catchup started");

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
            // we try with a different connection
            ws = match WSS.get_next_outbound() {
                None => bail!("can't find outbound connection"),
                Some(c) => c,
            };
        }
    }

    // now we are done the catchup
    COMING_ONLINE_TIME.store(::time::now() as usize, Ordering::Relaxed);

    // wait until there is no more working
    while UNIT_IN_WORK.get_waiter_num() != 0 {
        coroutine::sleep(Duration::from_secs(1));
    }
    WSS.request_free_joints_from_all_outbound_peers()?;
    error!("catchup done");
    Ok(())
}

/// this fn will be called every 8s in a timer
pub fn re_requeset_lost_joints(db: &Connection) -> Result<()> {
    let _g = match IS_CACTCHING_UP.try_lock() {
        Some(g) => g,
        None => return Ok(()),
    };

    let units = joint_storage::find_lost_joints(db)?;
    if units.is_empty() {
        return Ok(());
    }
    info!("lost units {:?}", units);

    let ws = match WSS.get_next_peer() {
        None => bail!("failed to find next peer"),
        Some(c) => c,
    };
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

fn notify_watchers(db: &Connection, joint: &Joint, cur_ws: &HubConn) -> Result<()> {
    let unit = &joint.unit;
    if unit.messages.is_empty() {
        return Ok(());
    }

    // already stable, light clients will require a proof
    if joint.ball.is_some() {
        return Ok(());
    }

    let mut addresses = unit.authors.iter().map(|a| &a.address).collect::<Vec<_>>();
    for message in &unit.messages {
        use spec::Payload;
        if message.app != "payment" || message.payload.is_none() {
            continue;
        }
        match message.payload {
            Some(Payload::Payment(ref payment)) => for output in &payment.outputs {
                let address = &output.address;
                if !addresses.contains(&address) {
                    addresses.push(address);
                }
            },
            _ => unreachable!("payload shoudl be a payment"),
        }
    }

    let addresses_str = addresses
        .into_iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT peer FROM watched_light_addresses WHERE address IN({})",
        addresses_str
    );

    let mut stmt = db.prepare(&sql)?;
    let rows = stmt
        .query_map(&[], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;

    if rows.is_empty() {
        return Ok(());
    }

    // light clients need timestamp
    let mut joint = joint.clone();
    joint.unit.timestamp = Some(::time::now() / 1000);

    for peer in rows {
        if let Some(ws) = WSS.get_connection_by_name(&peer) {
            if !ws.conn_eq(cur_ws) {
                ws.send_joint(&joint)?;
            }
        }
    }

    Ok(())
}

fn notify_light_clients_about_stable_joints(
    db: &Connection,
    from_mci: u32,
    to_mci: u32,
) -> Result<()> {
    let mut stmt = db.prepare_cached(
		"SELECT peer FROM units JOIN unit_authors USING(unit) JOIN watched_light_addresses USING(address) \
		WHERE main_chain_index>? AND main_chain_index<=? \
		UNION \
		SELECT peer FROM units JOIN outputs USING(unit) JOIN watched_light_addresses USING(address) \
		WHERE main_chain_index>? AND main_chain_index<=? \
		UNION \
		SELECT peer FROM units JOIN watched_light_units USING(unit) \
		WHERE main_chain_index>? AND main_chain_index<=?")?;

    let rows = stmt
        .query_map(
            &[&from_mci, &to_mci, &from_mci, &to_mci, &from_mci, &to_mci],
            |row| row.get(0),
        )?.collect::<::std::result::Result<Vec<String>, _>>()?;
    for peer in rows {
        if let Some(ws) = WSS.get_connection_by_name(&peer) {
            ws.send_just_saying("light/have_updates", Value::Null)?;
        }
    }

    let mut stmt = db.prepare_cached(
        "DELETE FROM watched_light_units \
         WHERE unit IN (SELECT unit FROM units WHERE main_chain_index>? AND main_chain_index<=?)",
    )?;

    stmt.execute(&[&from_mci, &to_mci])?;

    Ok(())
}

pub fn notify_watchers_about_stable_joints(mci: u32) -> Result<()> {
    use joint::WRITER_MUTEX;
    // the event was emitted from inside mysql transaction, make sure it completes so that the changes are visible
    // If the mci became stable in determineIfStableInLaterUnitsAndUpdateStableMcFlag (rare), write lock is released before the validation commits,
    // so we might not see this mci as stable yet. Hopefully, it'll complete before light/have_updates roundtrip
    let g = WRITER_MUTEX.lock().unwrap();
    // we don't need to block writes, we requested the lock just to wait that the current write completes
    drop(g);
    info!("notify_watchers_about_stable_joints, mci={} ", mci);
    if mci <= 1 {
        return Ok(());
    }
    let db = db::DB_POOL.get_connection();

    let last_ball_mci = storage::find_last_ball_mci_of_mci(&db, mci)?;
    let prev_last_ball_mci = storage::find_last_ball_mci_of_mci(&db, mci - 1)?;

    if last_ball_mci == prev_last_ball_mci {
        return Ok(());
    }

    notify_light_clients_about_stable_joints(&db, prev_last_ball_mci, last_ball_mci)
}
