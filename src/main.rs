#[macro_use]
extern crate failure;
#[macro_use]
extern crate serde_derive;

use std::collections::HashMap;

#[derive(Debug, Fail)]
enum ConsistentError {
    #[fail(display = "invalid toolchain name: {}", name)]
    InvalidToolchainName { name: String },
    #[fail(display = "unknown toolchain version: {}", version)]
    UnknownToolchainVersion { version: String },
}

fn main() {
    let c = MyC::new();
}

struct MyC {
    nodes: HashMap<u32, String>,
    count: u32,
}

impl Consistent for MyC {
    fn new() -> Self {
        unimplemented!()
    }
    fn add() {
        unimplemented!()
    }
    fn get() -> String {
        unimplemented!()
    }
    fn remove() {
        unimplemented!()
    }
}

trait Consistent {
    //=== regular consistent hash: https://github.com/stathat/consistent
    fn new() -> Self;
    fn add();
    fn get() -> String;
    fn remove();
    // func (c *Consistent) GetN(name string, n int) ([]string, error)
    // func (c *Consistent) GetTwo(name string) (string, string, error)
    // func (c *Consistent) Members() []string
    // func (c *Consistent) Set(elts []string)

    //==== for bounded loads: https://github.com/lafikl/consistent
    // func New() *Consistent
    // func (c *Consistent) Add(host string)
    // func (c *Consistent) Done(host string)
    // func (c *Consistent) Get(key string) (string, error)
    // func (c *Consistent) GetLeast(key string) (string, error)
    // func (c *Consistent) GetLoads() map[string]int64
    // func (c *Consistent) Hosts() (hosts []string)
    // func (c *Consistent) Inc(host string)
    // func (c *Consistent) MaxLoad() int64
    // func (c *Consistent) Remove(host string) bool
    // func (c *Consistent) UpdateLoad(host string, load int64)
}
