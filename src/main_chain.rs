use rusqlite::Connection;
// use spec::*;

pub fn determin_if_stable_in_laster_units_and_update_stable_mc_flag(
    db: &Connection,
    earlier_unit: &String,
    later_units: &[String],
    is_stable_in_db: u32, // this should be bool, but read from db
) -> bool {
    let _ = (db, earlier_unit, later_units, is_stable_in_db);
    unimplemented!()
}
