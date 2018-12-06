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
type Hash = u32;

struct MyCHash<N>
where
    Hashable: From<N>,
    N: Clone,
{
    /// map of Hash -> Node
    nodes: HashMap<Hash, N>,
    /// count of unique Nodes
    count: u32,
}

impl<N> MyCHash<N>
where
    Hashable: From<N>,
    N: Clone,
{
    fn calc_hash(&self, string: &[u8]) -> Hash {
        unimplemented!()
    }

    fn add_virtual_nodes(&mut self, node: N) {
        let v: Vec<u8> = node.clone().into();

        for v_node_num in 0..Self::REPLICAS {
            let mut v_clone = v.clone();

            // transform virtual node num to [u8]
            let b1: u8 = ((v_node_num >> 24) & 0xff) as u8;
            let b2: u8 = ((v_node_num >> 16) & 0xff) as u8;
            let b3: u8 = ((v_node_num >> 8) & 0xff) as u8;
            let b4: u8 = (v_node_num & 0xff) as u8;
            let mut bytes = [b1, b2, b3, b4].to_vec();

            // create a unique Vec<u8> per virtual node
            v_clone.append(&mut bytes);

            let hash = self.calc_hash(&v_clone);
            self.nodes.insert(hash, node.clone());
        }
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
        self.add_virtual_nodes(node);
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
