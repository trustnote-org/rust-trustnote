use error::Result;
use rusqlite::Connection;

pub fn purge_handled_balls_from_hash_tree(db: &Connection) -> Result<()> {
    let mut stmt = db.prepare_cached(
        "SELECT ball FROM hash_tree_balls \
         CROSS JOIN balls USING(ball)",
    )?;
    let balls = stmt.query_map(&[], |row| row.get::<_, String>(0))?;

    let mut stmt = db.prepare_cached("DELETE FROM hash_tree_balls WHERE ball=?")?;
    for ball in balls {
        stmt.execute(&[&ball?])?;
    }
    Ok(())
}
