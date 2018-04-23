use failure::Error;

#[derive(Debug, Fail)]
pub enum TrustnoteError {
    // TODO: need to define own error
    #[fail(display = "catchup prepare already current")]
    CatchupAlreadyCurrent,
    #[fail(display = "some witnesses have references in their addresses")]
    WitnessChanged,
}

pub type Result<T> = ::std::result::Result<T, Error>;
