use std::collections::VecDeque;
use std::rc::Rc;

use db;
use error::Result;
use joint::Joint;
use may::sync::Mutex;
use rusqlite::Connection;
use serde_json;
use storage;

#[derive(Debug)]
pub enum CheckNewResult {
    Known,
    KnownUnverified,
    KnownBad,
    New,
}

pub fn check_new_unit(db: &Connection, unit: &String) -> Result<CheckNewResult> {
    if storage::is_known_unit(unit) {
        return Ok(CheckNewResult::Known);
    }

    let mut stmt = db.prepare_cached("SELECT 1 FROM units WHERE unit=?")?;
    if stmt.exists(&[unit])? {
        storage::set_unit_is_known(unit);
        return Ok(CheckNewResult::Known);
    }

    let mut stmt = db.prepare_cached("SELECT 1 FROM unhandled_joints WHERE unit=?")?;
    if stmt.exists(&[unit])? {
        return Ok(CheckNewResult::KnownUnverified);
    }

    let mut stmt = db.prepare_cached("SELECT error FROM known_bad_joints WHERE unit=?")?;
    let mut rows = stmt.query(&[unit])?;
    if let Some(row) = rows.next() {
        let error: String = row?.get_checked(0)?;
        warn!("detect knownbad unit {}, err: {}", unit, error);
        return Ok(CheckNewResult::KnownBad);
    }

    Ok(CheckNewResult::New)
}

pub fn check_new_joint(db: &Connection, joint: &Joint) -> Result<CheckNewResult> {
    let unit = joint.get_unit_hash();
    let ret = check_new_unit(db, unit)?;
    if let CheckNewResult::New = ret {
        let mut stmt = db.prepare_cached("SELECT error FROM known_bad_joints WHERE joint=?")?;
        let joint_hash = joint.get_joint_hash();
        let mut rows = stmt.query(&[&joint_hash])?;
        if let Some(row) = rows.next() {
            let error: String = row?.get_checked(0)?;
            warn!("detect knownbad joint {}, err: {}", joint_hash, error);
            return Ok(CheckNewResult::KnownBad);
        }
    }
    Ok(ret)
}

pub fn remove_unhandled_joint_and_dependencies(db: &mut Connection, unit: &String) -> Result<()> {
    let tx = db.transaction()?;
    {
        let mut stmt = tx.prepare_cached("DELETE FROM unhandled_joints WHERE unit=?")?;
        stmt.execute(&[unit])?;

        let mut stmt = tx.prepare_cached("DELETE FROM dependencies WHERE unit=?")?;
        stmt.execute(&[unit])?;
    }
    tx.commit()?;
    Ok(())
}

pub fn save_unhandled_joint_and_dependencies(
    db: &mut Connection,
    joint: &Joint,
    missing_parent_units: &[String],
    peer: &String,
) -> Result<()> {
    let unit = joint.get_unit_hash();
    let tx = db.transaction()?;
    {
        let mut stmt =
            tx.prepare_cached("INSERT INTO unhandled_joints (unit, json, peer) VALUES (?, ?, ?)")?;
        stmt.execute(&[unit, &serde_json::to_string(joint)?, peer])?;
        let missing_units = missing_parent_units
            .iter()
            .map(|parent| format!("('{}', '{}')", unit, parent))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT OR IGNORE INTO dependencies (unit, depends_on_unit) VALUES {}",
            missing_units
        );
        let mut stmt = tx.prepare(&sql)?;
        stmt.execute(&[])?;
    }
    tx.commit()?;
    Ok(())
}

pub fn find_lost_joints(db: &Connection) -> Result<Vec<String>> {
    let mut stmt = db.prepare_cached(
        "SELECT DISTINCT depends_on_unit \
		FROM dependencies \
		LEFT JOIN unhandled_joints ON depends_on_unit=unhandled_joints.unit \
		LEFT JOIN units ON depends_on_unit=units.unit \
		WHERE unhandled_joints.unit IS NULL AND units.unit IS NULL AND dependencies.creation_date < \'NOW() + INTERVAL -8 SECOND\'"
        )?;

    let rows = stmt.query_map(&[], |row| row.get(0))?;

    let mut names = Vec::new();
    for depend_result in rows {
        names.push(depend_result?);
    }

    Ok(names)
}

pub fn read_joints_since_mci(db: &Connection, mci: u32) -> Result<Vec<Joint>> {
    let mut stmt = db.prepare_cached(
        "SELECT units.unit FROM units LEFT JOIN archived_joints USING(unit) \
		WHERE (is_stable=0 AND main_chain_index>=? OR main_chain_index IS NULL OR is_free=1) AND archived_joints.unit IS NULL \
		ORDER BY +level")?;

    let ret = stmt
        .query_map(&[&mci], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;

    let mut joints = Vec::new();
    for unit in ret {
        match storage::read_joint(db, &unit) {
            Ok(j) => joints.push(j),
            Err(e) => error!("read_joint err={}", e),
        }
    }

    Ok(joints)
}

#[derive(Debug)]
pub struct ReadyJoint {
    pub joint: Joint,
    pub create_ts: usize,
    pub peer: String,
}

pub fn read_dependent_joints_that_are_ready(
    db: &Connection,
    unit: Option<&String>,
) -> Result<(Vec<ReadyJoint>)> {
    let (from, where_clause) = if unit.is_some() {
        (
            "FROM dependencies AS src_deps JOIN dependencies USING(unit)",
            format!("WHERE src_deps.depends_on_unit='{}'", unit.unwrap()),
        )
    } else {
        ("FROM dependencies", String::new())
    };

    let sql = format!(
        "SELECT dependencies.unit, unhandled_joints.unit AS unit_for_json, \
         SUM(CASE WHEN units.unit IS NULL THEN 1 ELSE 0 END) AS count_missing_parents \
         {} \
         JOIN unhandled_joints ON dependencies.unit=unhandled_joints.unit \
         LEFT JOIN units ON dependencies.depends_on_unit=units.unit \
         {} \
         GROUP BY dependencies.unit \
         HAVING count_missing_parents=0 \
         ORDER BY NULL",
        from, where_clause
    );

    let mut ret = Vec::new();
    let mut stmt = db.prepare(&sql)?;
    let rows = stmt
        .query_map(&[], |row| row.get(1))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;

    for row in rows {
        let unit: String = row;
        let mut stmt = db.prepare_cached(
            "SELECT json, peer, strftime('%s', creation_date) AS creation_ts FROM unhandled_joints WHERE unit=?")?;

        let mut rows_inner = stmt
            .query_map(&[&unit], |row_inner| ReadyJoint {
                joint: serde_json::from_str(&row_inner.get::<_, String>(0))
                    .expect("failed to parse json"),
                create_ts: row_inner.get::<_, String>(2).parse::<usize>().unwrap() * 1000,
                peer: row_inner.get(1),
            })?.collect::<::std::result::Result<Vec<ReadyJoint>, _>>()?;

        ret.append(rows_inner.as_mut());
    }

    Ok(ret)
}

pub fn purge_joint_and_dependencies<F>(
    db: &mut Connection,
    joint: &Joint,
    err: &str,
    f: F,
) -> Result<()>
where
    F: Fn(&str, &str, &str) + 'static,
{
    let unit = joint.get_unit_hash();
    let rc_unit = Rc::new(unit.clone());

    let tx = db.transaction()?;
    {
        let mut stmt =
            tx.prepare_cached("INSERT INTO known_bad_joints (unit, json, error) VALUES (?, ?, ?)")?;
        stmt.execute(&[unit, &serde_json::to_string(joint)?, &err])?;

        let mut stmt = tx.prepare_cached("DELETE FROM unhandled_joints WHERE unit=?")?;
        stmt.execute(&[unit])?;

        let mut stmt = tx.prepare_cached("DELETE FROM dependencies WHERE unit=?")?;
        stmt.execute(&[unit])?;
    }

    let mut queries = db::DbQueries::new();

    collet_queries_to_purge_dependent_joints(&tx, rc_unit, &mut queries, err, f)?;

    queries.execute(&tx)?;
    tx.commit()?;

    Ok(())
}

fn collet_queries_to_purge_dependent_joints<F>(
    db: &Connection,
    unit: Rc<String>,
    queries: &mut db::DbQueries,
    err: &str,
    f: F,
) -> Result<()>
where
    F: Fn(&str, &str, &str) + 'static,
{
    struct TempUnitProp {
        unit: String,
        peer: String,
    }

    let mut deque = VecDeque::new();
    deque.push_back(TempUnitProp {
        unit: unit.to_string(),
        peer: String::from("unknow"),
    });

    while let Some(new_unit) = deque.pop_front() {
        let mut stmt = db.prepare_cached("SELECT unit, peer FROM dependencies JOIN unhandled_joints USING(unit) WHERE depends_on_unit=?",)?;

        let unit_rows = stmt
            .query_map(&[&new_unit.unit], |row| TempUnitProp {
                unit: row.get(0),
                peer: row.get(1),
            })?.collect::<::std::result::Result<Vec<_>, _>>()?;

        let units_str = unit_rows
            .iter()
            .map(|s| format!("'{}'", s.unit))
            .collect::<Vec<_>>()
            .join(", ");

        for row in unit_rows {
            deque.push_back(row);
        }
        let err_str = err.to_owned();

        queries.add_query(move |db| {
            let sql = format!(
                "INSERT OR IGNORE INTO known_bad_joints (unit, json, error) \
                 SELECT unit, json, ? FROM unhandled_joints WHERE unit IN({})",
                units_str
            );
            let mut stmt = db.prepare(&sql)?;
            stmt.execute(&[&err_str])?;

            let sql = format!("DELETE FROM unhandled_joints WHERE unit IN({})", units_str);
            let mut stmt = db.prepare(&sql)?;
            stmt.execute(&[])?;

            let sql = format!("DELETE FROM dependencies WHERE unit IN({})", units_str);
            let mut stmt = db.prepare(&sql)?;
            stmt.execute(&[])?;
            Ok(())
        });

        f(&new_unit.unit, &new_unit.peer, err);
    }
    Ok(())
}

fn purge_uncovered_nonserial_joints(mut by_existence_of_children: bool) -> Result<()> {
    use joint::WRITER_MUTEX;
    let mut db = db::DB_POOL.get_connection();

    loop {
        let units = {
            let mut stmt = if by_existence_of_children {
                // by_existence_of_children = true
                db.prepare_cached(
                "SELECT unit FROM units INDEXED BY bySequence \
                 WHERE (SELECT 1 FROM parenthoods WHERE parent_unit=unit LIMIT 1) IS NULL AND sequence IN('final-bad','temp-bad') AND content_hash IS NULL \
                     AND NOT EXISTS (SELECT * FROM dependencies WHERE depends_on_unit=units.unit) \
                     AND NOT EXISTS (SELECT * FROM balls WHERE balls.unit=units.unit) \
                     AND EXISTS ( \
                         SELECT DISTINCT address FROM units AS wunits CROSS \
                         JOIN unit_authors USING(unit) CROSS JOIN my_witnesses USING(address) \
                         WHERE wunits.rowid > units.rowid \
                         LIMIT 6,1 \
                     )",
                // FIXME: LIMIT = config::MAJORITY_OF_WITNESSES - 1;
                )?
            } else {
                db.prepare_cached(
                "SELECT unit FROM units \
                 WHERE is_free=1 AND sequence IN('final-bad','temp-bad') AND content_hash IS NULL \
                     AND NOT EXISTS (SELECT * FROM dependencies WHERE depends_on_unit=units.unit) \
                     AND NOT EXISTS (SELECT * FROM balls WHERE balls.unit=units.unit) \
                     AND EXISTS ( \
                         SELECT DISTINCT address FROM units AS wunits CROSS \
                         JOIN unit_authors USING(unit) CROSS JOIN my_witnesses USING(address) \
                         WHERE wunits.rowid > units.rowid LIMIT 6,1 \
                     )",
                // FIXME: LIMIT = config::MAJORITY_OF_WITNESSES - 1;
                )?
            };
            let rows = stmt.query_map(&[], |row| row.get(0))?;
            rows.collect::<::std::result::Result<Vec<String>, _>>()?
        };

        if units.is_empty() {
            if !by_existence_of_children {
                return Ok(());
            } else {
                break;
            }
        }

        for unit in units {
            info!("--------------- archiving uncovered unit {}", unit);
            let joint = storage::read_joint(&db, &unit)
                .or_else(|e| bail!("nonserial unit not found?, err={}", e))?;

            let g = WRITER_MUTEX.lock().unwrap();
            let mut queries = db::DbQueries::new();
            storage::generate_queries_to_archive_joint(
                &db,
                &joint,
                storage::ArchiveJointReason::Uncovered,
                &mut queries,
            )?;
            let tx = db.transaction()?;
            queries.execute(&tx)?;
            tx.commit()?;
            drop(g);
            storage::forget_unit(&unit);
        }

        by_existence_of_children = true;
    }

    if !by_existence_of_children {
        return Ok(());
    }

    let mut stmt = db.prepare_cached(
        "UPDATE units SET is_free=1 WHERE is_free=0 AND main_chain_index IS NULL \
         AND (SELECT 1 FROM parenthoods WHERE parent_unit=unit LIMIT 1) IS NULL",
    )?;
    stmt.execute(&[])?;

    Ok(())
}

pub fn purge_uncovered_nonserial_joints_under_lock() -> Result<()> {
    lazy_static! {
        static ref PURGE_UNCOVERED: Mutex<()> = Mutex::new(());
    }

    if PURGE_UNCOVERED.try_lock().is_ok() {
        return purge_uncovered_nonserial_joints(false);
    }
    Ok(())
}
