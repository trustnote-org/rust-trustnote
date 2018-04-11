use failure::Error;

#[derive(Debug, Fail)]
pub enum TrustnoteError {
    // TODO: need to define own error
    #[fail(display = "invalid toolchain name: {}", name)]
    InvalidToolchainName { name: String },
    #[fail(display = "unknown toolchain version: {}", version)]
    UnknownToolchainVersion { version: String },
}

pub type Result<T> = ::std::result::Result<T, Error>;
