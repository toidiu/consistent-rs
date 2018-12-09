#[macro_use]
extern crate failure;
#[macro_use]
extern crate serde_derive;

use std::collections::HashMap;
use std::str::FromStr;

use blake2::digest::{Input, VariableOutput};
use blake2::{Blake2b, VarBlake2b};
// use blake2::{Blake2b, Digest};

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
    pub fn as_slice_u8_be(num: u32) -> [u8; 4] {
        let b1: u8 = ((num >> 24) & 0xff) as u8;
        let b2: u8 = ((num >> 16) & 0xff) as u8;
        let b3: u8 = ((num >> 8) & 0xff) as u8;
        let b4: u8 = (num & 0xff) as u8;
        [b1, b2, b3, b4]
    }

    pub fn as_u32_be(array: &[u8; 4]) -> u32 {
        ((array[0] as u32) << 24)
            + ((array[1] as u32) << 16)
            + ((array[2] as u32) << 8)
            + ((array[3] as u32) << 0)
    }

    fn internal_calc_hash(data: &[u8]) -> Hash {
        let mut hasher = VarBlake2b::new(4).unwrap();
        hasher.input(data);
        let res = hasher.vec_result();
        println!("{:?}", &res);
        assert_eq!(res.len(), 4);
        let v: [u8; 4] = unsafe {
            [
                res.get_unchecked(0).clone(),
                res.get_unchecked(1).clone(),
                res.get_unchecked(2).clone(),
                res.get_unchecked(3).clone(),
            ]
        };

        Self::as_u32_be(&v)
    }

    fn calc_v_hash(v: &mut Vec<u8>, v_idx: u32) -> u32 {
        // transform v_idx to [u8]
        let mut bytes = Self::as_slice_u8_be(v_idx).to_vec();
        // create a unique Vec<u8> per virtual index
        v.append(&mut bytes);
        Self::internal_calc_hash(&v)
    }

    fn add_virtual_nodes(&mut self, node: N) {
        let v: Vec<u8> = node.clone().into();

        for v_idx in 0..Self::REPLICAS {
            // add v_idx to the node to create a unique key
            let mut v_clone = v.clone();

            let hash = Self::calc_v_hash(&mut v_clone, v_idx);
            println!("{}", hash);
            self.nodes.insert(hash, node.clone());
        }
    }

    fn remove_virtual_nodes(&mut self, node: &N) {
        let v: Vec<u8> = node.clone().into();

        for v_idx in 0..Self::REPLICAS {
            // add v_idx to the node to create a unique key
            let mut v_clone = v.clone();

            let hash = Self::calc_v_hash(&mut v_clone, v_idx);
            println!("{}", hash);
            self.nodes.remove(&hash);
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
        MyCHash {
            nodes: HashMap::new(),
            count: 0,
        }
    }
    fn add(&mut self, node: Self::NodeType) {
        let mut v: Vec<u8> = node.clone().into();
        let contains_hash = Self::calc_v_hash(&mut v, 0);

        if (!self.nodes.contains_key(&contains_hash)) {
            self.count += 1;
            self.add_virtual_nodes(node);
        }
    }
    fn remove(&mut self, node: &Self::NodeType) {
        let mut v: Vec<u8> = node.clone().into();
        let contains_hash = Self::calc_v_hash(&mut v, 0);

        if (self.nodes.contains_key(&contains_hash)) {
            self.count -= 1;
            self.remove_virtual_nodes(node);
        }
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
    fn remove(&mut self, node: &Self::NodeType);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        let mut c = MyCHash::<String>::new();
        assert_eq!(c.count, 0);
        c.add("a".to_string());
        assert_eq!(c.count, 1);
        c.add("b".to_string());
        assert_eq!(c.count, 2);
    }

    #[test]
    fn test_remove() {
        let mut c = MyCHash::<String>::new();
        assert_eq!(c.count, 0);
        c.add("a".to_string());
        assert_eq!(c.count, 1);
        c.add("b".to_string());
        assert_eq!(c.count, 2);
        c.remove(&"a".to_string());
        assert_eq!(c.count, 1);
        c.remove(&"b".to_string());
        assert_eq!(c.count, 0);
    }

}
