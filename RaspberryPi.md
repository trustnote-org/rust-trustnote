# Building TrustNote Rust SDK From Source for Raspberry Pi

*Note: The following procedures were tested to work for both raspberry pi 2b and 2b+.*

## Build for the host (Ubuntu)

### Install Rust
```
curl https://sh.rustup.rs -sSf | sh
```

### Config your current shell
```
source $HOME/.cargo/env
```

### Clone TrustNote Rust SDK
```
git clone https://github.com/trustnote/rust-trustnote.git
```

### Build the project
```
cargo build
```

*Note: You may need to run ```sudo apt install libssl-dev``` if you see error messages like this:*
```
error: failed to run custom build command for `openssl-sys v0.9.35`
```

## Cross Compile

### Install the environment
```
sudo apt-get install make git-core ncurses-dev gcc-arm*
```

### Configure cargo for cross compilation
```
cd ttt
nano .cargo/config
```

### Edit the contents:
```
[target.arm-unknown-linux-gnueabi]
linker = "arm-linux-gnueabi-gcc"
ar = "arm-linux-gnueabi-gcc"
```

### Install the cross compiled standard crates
```
rustup target add armv7-unknown-linux-gnueabihf
```

### Build the project for the target
```
cargo build --target arm-unknown-linux-gnueabi
```

If successful, you will find the executable ttt from ```target/arm-unknown-linux-gnueabi/debug/``` where you can deploy the binary to the target.






