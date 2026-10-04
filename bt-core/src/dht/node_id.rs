use std::cmp::Ordering;

pub const NODE_ID_LENGTH: usize = 20;

pub trait RandomBytes: Send + Sync {
    fn fill(&self, dest: &mut [u8]);
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemRandom;

impl RandomBytes for SystemRandom {
    fn fill(&self, dest: &mut [u8]) {
        use rand::RngCore;
        rand::rng().fill_bytes(dest);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId([u8; NODE_ID_LENGTH]);

impl NodeId {
    pub const fn from_bytes(bytes: [u8; NODE_ID_LENGTH]) -> NodeId {
        NodeId(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; NODE_ID_LENGTH] {
        &self.0
    }

    pub fn random(source: &dyn RandomBytes) -> NodeId {
        let mut bytes = [0u8; NODE_ID_LENGTH];
        source.fill(&mut bytes);
        NodeId(bytes)
    }
}

pub fn distance(a: &NodeId, b: &NodeId) -> [u8; NODE_ID_LENGTH] {
    let mut result = [0u8; NODE_ID_LENGTH];
    for (index, (left, right)) in a.as_bytes().iter().zip(b.as_bytes()).enumerate() {
        result[index] = left ^ right;
    }
    result
}

pub fn bucket_index(distance: &[u8; NODE_ID_LENGTH]) -> Option<u8> {
    let first_set = distance.iter().position(|&byte| byte != 0)?;
    let leading_zeros = first_set * 8 + distance[first_set].leading_zeros() as usize;
    Some((NODE_ID_LENGTH * 8 - 1 - leading_zeros) as u8)
}

pub fn cmp_distance_to(a: &NodeId, b: &NodeId, target: &NodeId) -> Ordering {
    distance(a, target).cmp(&distance(b, target))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FixedRandom(Mutex<Vec<u8>>);

    impl RandomBytes for FixedRandom {
        fn fill(&self, dest: &mut [u8]) {
            let mut source = self.0.lock().unwrap();
            for byte in dest.iter_mut() {
                *byte = if source.is_empty() {
                    0
                } else {
                    source.remove(0)
                };
            }
        }
    }

    fn id(text: &str) -> NodeId {
        let mut bytes = [0u8; NODE_ID_LENGTH];
        bytes.copy_from_slice(&text.as_bytes()[..NODE_ID_LENGTH]);
        NodeId::from_bytes(bytes)
    }

    #[test]
    fn random_node_ids_differ() {
        let first = NodeId::random(&SystemRandom);
        let second = NodeId::random(&SystemRandom);
        assert_ne!(first, second);
        assert_ne!(first.as_bytes(), second.as_bytes());
    }

    #[test]
    fn node_id_random_consumes_source_bytes() {
        let source = FixedRandom::default();
        let node = NodeId::random(&source);
        assert_eq!(node.as_bytes(), &[0u8; NODE_ID_LENGTH]);
    }

    #[test]
    fn distance_is_xor() {
        let a = id("abcdefghij0123456789");
        let b = id("abcdefghij0123456788");
        let result = distance(&a, &b);
        assert_eq!(result[NODE_ID_LENGTH - 1], 0x01);
        assert_eq!(bucket_index(&result), Some(0));
    }

    #[test]
    fn distance_to_self_is_zero_and_has_no_bucket() {
        let a = id("abcdefghij0123456789");
        assert_eq!(distance(&a, &a), [0u8; NODE_ID_LENGTH]);
        assert_eq!(bucket_index(&distance(&a, &a)), None);
    }

    #[test]
    fn bucket_index_spans_the_full_range() {
        let mut far = [0u8; NODE_ID_LENGTH];
        far[0] = 0x80;
        assert_eq!(bucket_index(&far), Some(159));

        let mut near = [0u8; NODE_ID_LENGTH];
        near[0] = 0x01;
        assert_eq!(bucket_index(&near), Some(152));

        let mut last = [0u8; NODE_ID_LENGTH];
        last[NODE_ID_LENGTH - 1] = 0x01;
        assert_eq!(bucket_index(&last), Some(0));

        let mut second = [0u8; NODE_ID_LENGTH];
        second[NODE_ID_LENGTH - 1] = 0x80;
        assert_eq!(bucket_index(&second), Some(7));
    }

    #[test]
    fn closer_nodes_compare_less() {
        let target = id("abcdefghij0123456789");
        let near = id("abcdefghij0123456788");
        let far = id("ffffffffffffffffff00");
        assert_eq!(cmp_distance_to(&near, &far, &target), Ordering::Less);
        assert_eq!(cmp_distance_to(&far, &near, &target), Ordering::Greater);
        assert_eq!(cmp_distance_to(&near, &near, &target), Ordering::Equal);
    }
}
