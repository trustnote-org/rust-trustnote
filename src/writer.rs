use error::Result;
use joint::Joint;

// pub struct TempValidationState{
//     sequence: Option<String>,

// }
pub fn save_joint(joint: &Joint, sequence: String) -> Result<()> {
    let joint_unit = joint.clone().unit;
    info!("saving unit {}", joint_unit.unit.unwrap());
    //TODO: need to be impl
    Ok(())
}
