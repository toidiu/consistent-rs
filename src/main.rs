#[macro_use]
extern crate failure;
#[macro_use]
extern crate serde_derive;

use std::collections::BTreeMap;

use blake2::digest::FixedOutput;
use blake2::Blake2s;
use blake2::Digest;

#[derive(Debug, Fail)]
enum ConsistentError {
    #[fail(display = "invalid toolchain name: {}", name)]
    InvalidToolchainName { name: String },
    #[fail(display = "unknown toolchain version: {}", version)]
    UnknownToolchainVersion { version: String },
}

type AppResult<T> = Result<ConsistentError, T>;

fn main() {
    let _c = MyCHash::<String>::new();
}

type Hashable = Vec<u8>;
type Hash = u64;

struct MyCHash<N>
where
    Hashable: From<N>,
    N: Clone,
{
    /// This is used to retrieve the closest server to a hashed item.
    /// The properties we would want from this structure are:
    /// - fast search: find entry with the closest hash to item
    /// - fast insert: add an entry
    /// - fast removes: remove an entry based on a virtual hash
    /// - fast lookup: get the server value based on its virtual hash
    ///
    /// Based on these criterias the ideal structure seems to be
    /// `BTreeHash<Hash, N>`. The structure automatically maintains
    /// the entries in sorted order, which gives us O(n*log(n)) for searches.
    /// It also supports O(n*log(n)) inserts, removes, and lookups.
    nodes: BTreeMap<Hash, N>,

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

    fn internal_calc_hash(data: &[u8]) -> Hash {
        let mut hasher = Blake2s::new();
        hasher.input(data);
        let res = hasher.fixed_result();
        assert!(res.len() == 32);

        ((res[0] as u64) << 56)
            + ((res[1] as u64) << 48)
            + ((res[2] as u64) << 40)
            + ((res[3] as u64) << 32)
            + ((res[4] as u64) << 24)
            + ((res[5] as u64) << 16)
            + ((res[6] as u64) << 8)
            + ((res[7] as u64) << 0)
    }

    fn calc_v_hash(v: &Vec<u8>, v_idx: u32) -> Hash {
        // spread the virtual index
        let idx_bytes = Self::as_slice_u8_be(v_idx * Self::MUL).to_vec();

        // modify res
        let mut res = Vec::new();
        res.append(&mut idx_bytes.clone());
        res.append(&mut v.clone());
        res.append(&mut idx_bytes.clone());

        // create a unique Vec<u8> per virtual index
        Self::internal_calc_hash(&res)
    }

    fn add_virtual_nodes(&mut self, node: N) {
        let v: Vec<u8> = node.clone().into();

        for v_idx in 0..Self::REPLICAS {
            // add v_idx to the node to create a unique key
            let hash = Self::calc_v_hash(&v, v_idx);
            self.nodes.insert(hash, node.clone());
        }
    }

    fn remove_virtual_nodes(&mut self, node: &N) {
        let v: Vec<u8> = node.clone().into();

        for v_idx in 0..Self::REPLICAS {
            // add v_idx to the node to create a unique key
            let hash = Self::calc_v_hash(&v, v_idx);
            self.nodes.remove(&hash);
        }
    }

    fn get_virtual_node(&self, item: &N) -> N {
        // item hash
        let v: Vec<u8> = item.clone().into();
        let i_hash = Self::internal_calc_hash(&v);

        // get num of entries
        let num_entries = self.nodes.len();

        // get vec of tuple of key and value and then do a binary search
        let sorted_vec: Vec<(&Hash, &N)> = self.nodes.iter().collect();
        let cmp_res: Result<usize, usize> = sorted_vec.binary_search_by(|i| (i.0).cmp(&i_hash));

        // get the index at which the item lands
        let item_idx: usize = match cmp_res {
            Ok(idx) => idx,
            Err(idx) => {
                if idx >= num_entries {
                    0
                } else {
                    idx
                }
            }
        };

        let res: &(&Hash, &N) = sorted_vec
            .get(item_idx)
            .expect("expected idx to be present in sorted_nodes");

        res.clone().1.clone()
    }
}

impl<'a, N> Consistent<'a> for MyCHash<N>
where
    Hashable: From<N>,
    N: Clone,
{
    type HashableItem = N;

    /// create a new Consistent Hash instance
    fn new() -> Self {
        MyCHash {
            nodes: BTreeMap::new(),
            count: 0,
        }
    }

    /// add servers from list
    fn add(&mut self, node: Self::HashableItem) {
        let v: Vec<u8> = node.clone().into();
        let contains_hash = Self::calc_v_hash(&v, 0);

        if !self.nodes.contains_key(&contains_hash) {
            self.count += 1;
            self.add_virtual_nodes(node);
        }
    }

    /// remove servers from list
    fn remove(&mut self, node: &Self::HashableItem) {
        let v: Vec<u8> = node.clone().into();
        let contains_hash = Self::calc_v_hash(&v, 0);

        if self.nodes.contains_key(&contains_hash) {
            self.count -= 1;
            self.remove_virtual_nodes(node);
        }
    }

    /// for a given `item` return a server which will handle its request
    fn get(&self, item: &Self::HashableItem) -> Self::HashableItem {
        self.get_virtual_node(item)
    }
}

trait Consistent<'a> {
    type HashableItem;

    const REPLICAS: u32 = 11;
    const MUL: u32 = 7;
    //=== regular consistent hash: https://github.com/stathat/consistent
    fn new() -> Self;
    fn add(&mut self, node: Self::HashableItem);
    fn remove(&mut self, node: &Self::HashableItem);
    fn get(&self, item: &Self::HashableItem) -> Self::HashableItem;
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
    fn add() {
        let mut c = MyCHash::<&str>::new();
        assert_eq!(c.count, 0);
        c.add("a");
        assert_eq!(c.count, 1);
        c.add("b");
        assert_eq!(c.count, 2);
    }

    #[test]
    fn remove() {
        let mut c = MyCHash::<&str>::new();
        assert_eq!(c.count, 0);
        c.add("a");
        assert_eq!(c.count, 1);
        c.add("b");
        assert_eq!(c.count, 2);
        c.remove(&"a");
        assert_eq!(c.count, 1);
        c.remove(&"b");
        assert_eq!(c.count, 0);
    }

    #[test]
    fn only_move_keys_that_were_removed() {
        let mut ch = MyCHash::<&str>::new();
        ch.add("server1");
        ch.add("server2");
        ch.add("server3");
        ch.add("server4");
        ch.add("server5");
        ch.add("server6");

        println!("{:?}", ch.get(&"item1"));
        println!("{:?}", ch.get(&"item2"));
        println!("{:?}", ch.get(&"item3"));
        println!("{:?}", ch.get(&"item4"));
        println!("{:?}", ch.get(&"item5"));
        println!("{:?}", ch.get(&"item6"));
        println!("{:#?}", ch.nodes);

        let oneh = ch.get(&"item1");
        let twoh = ch.get(&"item2");

        ch.remove(&oneh);
        assert_ne!(ch.get(&"item1"), oneh);
        assert_eq!(ch.get(&"item2"), twoh);
    }

}
