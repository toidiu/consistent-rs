#[macro_use]
extern crate failure;
extern crate serde_derive;

use std::collections::BTreeMap;

use blake2::digest::FixedOutput;
use blake2::Blake2s;
use blake2::Digest;

#[derive(Debug, Fail)]
enum ConsistentError {
    #[fail(display = "there are no server nodes currently registered in the hasher")]
    NoServerNodes,
}

type AppResult<T> = Result<T, ConsistentError>;

type Hashable = Vec<u8>;
type Hash = u64;

pub struct ConsistentHash<Node>
where
    Hashable: From<Node>,
    Node: Clone,
{
    /// This is used to retrieve the closest server to a hashed item.
    /// The properties we would want from this structure are:
    /// - fast search: find entry with the closest hash to item
    /// - fast insert: add an entry
    /// - fast removes: remove an entry based on a virtual hash
    /// - fast lookup: get the server value based on its virtual hash
    ///
    /// Based on these criterias the ideal structure seems to be
    /// `BTreeHash<Hash, Node>`. The structure automatically maintains
    /// the entries in sorted order, which gives us O(n*log(n)) for searches.
    /// It also supports O(n*log(n)) inserts, removes, and lookups.
    nodes: BTreeMap<Hash, Node>,

    /// count of unique Nodes
    count: u32,
}

impl<Node> ConsistentHash<Node>
where
    Hashable: From<Node>,
    Node: Clone,
{
    /// create a new Consistent Hash instance
    pub fn new() -> Self {
        ConsistentHash {
            nodes: BTreeMap::new(),
            count: 0,
        }
    }

    #[inline]
    fn u32_to_bytes_be(u: u32) -> [u8; 4] {
        let a: [u8; 4] = unsafe { std::mem::transmute(u32::to_be(u)) };
        a
    }

    fn internal_calc_hash(data: &[u8]) -> Hash {
        let mut hasher = Blake2s::new();
        hasher.input(data);
        let res = hasher.fixed_result();
        assert!(res.len() == 32, "output should be [u8; 32]");

        // return type u64 constructed from [u8; 32]
        ((res[0] as u64) << 56)
            + ((res[1] as u64) << 48)
            + ((res[2] as u64) << 40)
            + ((res[3] as u64) << 32)
            + ((res[4] as u64) << 24)
            + ((res[5] as u64) << 16)
            + ((res[6] as u64) << 8)
            + ((res[7] as u64) << 0)
    }

    fn calc_v_hash(v: &[u8], v_idx: &u32) -> Hash {
        // spread the virtual index
        let idx_bytes: &[u8] = &Self::u32_to_bytes_be(v_idx * Self::MUL)[..];

        // create virtual nodes
        let ad: &[u8] = &[idx_bytes, v, idx_bytes].concat();

        // create a unique Vec<u8> per virtual index
        Self::internal_calc_hash(&ad)
    }

    fn add_virtual_nodes(&mut self, node: &Node) {
        let v: Hashable = node.clone().into();

        for v_idx in Self::INIT_V_IDX..Self::REPLICAS {
            // add v_idx to the node to create a unique key
            let hash = Self::calc_v_hash(&v, &v_idx);
            self.nodes.insert(hash, node.clone());
        }
    }

    fn remove_virtual_nodes(&mut self, node: &Node) {
        let v: Hashable = node.clone().into();

        for v_idx in Self::INIT_V_IDX..Self::REPLICAS {
            // add v_idx to the node to create a unique key
            let hash = Self::calc_v_hash(&v, &v_idx);
            self.nodes.remove(&hash);
        }
    }

    fn get_virtual_node(&self, item: &Node) -> Node {
        // item hash
        let v: Hashable = item.clone().into();
        let i_hash = Self::internal_calc_hash(&v);

        // get num of entries
        let num_entries = self.nodes.len();

        // get vec of tuple of key and value and then do a binary search
        let sorted_vec: Vec<(&Hash, &Node)> = self.nodes.iter().collect();
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

        let res: &(&Hash, &Node) = sorted_vec
            .get(item_idx)
            .expect("expected idx to be present in sorted_nodes");

        res.1.clone()
    }
}

impl<'a, Node> Consistent<'a> for ConsistentHash<Node>
where
    Hashable: From<Node>,
    Node: Clone,
{
    type HashableItem = Node;

    /// add servers from list
    fn add(&mut self, node: Self::HashableItem) {
        let v: Hashable = node.clone().into();
        let contains_hash = Self::calc_v_hash(&v, &Self::INIT_V_IDX);

        if !self.nodes.contains_key(&contains_hash) {
            self.count += 1;
            self.add_virtual_nodes(&node);
        }
    }

    /// remove servers from list
    fn remove(&mut self, node: Self::HashableItem) {
        let v: Hashable = node.clone().into();
        let contains_hash = Self::calc_v_hash(&v, &Self::INIT_V_IDX);

        if self.nodes.contains_key(&contains_hash) {
            self.count -= 1;
            self.remove_virtual_nodes(&node);
        }
    }

    /// for a given `item` return a server which will handle its request
    fn get(&self, item: &Self::HashableItem) -> AppResult<Self::HashableItem> {
        if self.nodes.len() > 0 {
            Ok(self.get_virtual_node(item))
        } else {
            Err(ConsistentError::NoServerNodes)
        }
    }

    /// get the number of nodes that are currently registered
    fn get_node_count(&self) -> u32 {
        self.count
    }
}

trait Consistent<'a> {
    type HashableItem;

    const REPLICAS: u32 = 11;
    const MUL: u32 = 7;
    const INIT_V_IDX: u32 = 0;

    //=== regular consistent hash: https://github.com/stathat/consistent
    // fn new() -> Self;
    fn add(&mut self, node: Self::HashableItem);
    fn remove(&mut self, node: Self::HashableItem);
    fn get(&self, item: &Self::HashableItem) -> AppResult<Self::HashableItem>;
    fn get_node_count(&self) -> u32;
    // func (c *Consistent) GetN(name string, n int) ([]string, error)
    // func (c *Consistent) GetTwo(name string) (string, string, error)
    // func (c *Consistent) Members() []string
    // func (c *Consistent) Set(elts []string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add() {
        let mut c = ConsistentHash::<&str>::new();
        assert_eq!(c.count, 0);
        c.add("a");
        assert_eq!(c.count, 1);
        c.add("b");
        assert_eq!(c.count, 2);
    }

    #[test]
    fn remove() {
        let mut c = ConsistentHash::<&str>::new();
        assert_eq!(c.count, 0);
        c.add("a");
        assert_eq!(c.count, 1);
        c.add("b");
        assert_eq!(c.count, 2);
        c.remove("a");
        assert_eq!(c.count, 1);
        c.remove("b");
        assert_eq!(c.count, 0);
    }

    #[test]
    fn only_move_keys_that_were_removed() {
        let mut ch = ConsistentHash::<&str>::new();
        ch.add("server1");
        ch.add("server2");
        ch.add("server3");
        ch.add("server4");
        ch.add("server5");
        ch.add("server6");

        let oneh = ch.get(&"item1").unwrap();
        let twoh = ch.get(&"item2").unwrap();

        ch.remove(oneh);
        assert_ne!(ch.get(&"item1").unwrap(), oneh);
        assert_eq!(ch.get(&"item2").unwrap(), twoh);
    }

    #[test]
    fn get_node_count() {
        let mut ch = ConsistentHash::<&str>::new();
        assert_eq!(ch.get_node_count(), 0);
        ch.add("server1");
        assert_eq!(ch.get_node_count(), 1);
        ch.add("server2");
        assert_eq!(ch.get_node_count(), 2);
        ch.remove("server1");
        assert_eq!(ch.get_node_count(), 1);
    }

    #[test]
    fn err_on_empty_nodes() {
        let ch = ConsistentHash::<&str>::new();
        assert!(ch.get(&"item1").is_err());
    }
}
