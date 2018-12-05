#[macro_use]
extern crate failure;
#[macro_use]
extern crate serde_derive;

use std::collections::HashMap;
use std::str::FromStr;

#[derive(Debug, Fail)]
enum ConsistentError {
    #[fail(display = "invalid toolchain name: {}", name)]
    InvalidToolchainName { name: String },
    #[fail(display = "unknown toolchain version: {}", version)]
    UnknownToolchainVersion { version: String },
}

type AppResult<T> = Result<ConsistentError, T>;

fn main() {
    let c = MyCHash::<String>::new();
}

type Hashable = Vec<u8>;

struct MyCHash<N>
where
    Hashable: From<N>,
    N: Clone,
{
    nodes: HashMap<u32, N>,
    count: u32,
}

impl<N> MyCHash<N>
where
    Hashable: From<N>,
    N: Clone,
{
    fn get_hash(&self, string: &N, replica_num: u32) -> u32 {
        unimplemented!()
    }

    fn get_virtual_nodes() {
        unimplemented!()
    }
}

impl<'a, N> Consistent<'a> for MyCHash<N>
where
    Hashable: From<N>,
    N: Clone,
{
    type NodeType = N;

    fn new() -> Self {
        unimplemented!()
    }
    fn add(&mut self, node: Self::NodeType) {
        self.count += 1;
        for i in 0..Self::REPLICAS {
            let hash = self.get_hash(&node, i);
            self.nodes.insert(hash, node.clone());
        }
    }
    fn remove() {
        unimplemented!()
    }
    fn get() -> Self::NodeType {
        unimplemented!()
    }
}

trait Consistent<'a> {
    type NodeType;

    const REPLICAS: u32 = 10;
    //=== regular consistent hash: https://github.com/stathat/consistent
    fn new() -> Self;
    fn add(&mut self, node: Self::NodeType);
    fn remove();
    fn get() -> Self::NodeType;
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
