use failure::Error;

macro_rules! bail_close {
    ($fmt:expr, $($arg:tt)*) => {
        return Err($crate::TrustnoteError::WsClose{ msg: format!($fmt, $($arg)*)}.into());
    };
}

#[derive(Debug, Fail)]
pub enum TrustnoteError {
    // TODO: need to define own error
    #[fail(display = "catchup prepare already current")]
    CatchupAlreadyCurrent,
    #[fail(display = "some witnesses have references in their addresses")]
    WitnessChanged,
    #[fail(display = "need to close the connection: {}", msg)]
    WsClose { msg: String },
}

pub type Result<T> = ::std::result::Result<T, Error>;
