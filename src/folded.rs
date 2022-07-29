use xxhash_rust::xxh3::xxh3_64_with_seed;

// M = S * F
#[derive(Debug)]
pub struct Folded<const F: usize, const S: usize, const K: usize> {
    pub bytes: [u8; S],
}

impl<const F: usize, const S: usize, const K: usize> Folded<F, S, K> {
    pub fn new() -> Self {
        Self { bytes: [0u8; S] }
    }

    pub fn insert<H: AsRef<[u8]>>(&mut self, hash: &H) {
        for index in Self::build_expected(hash).folded(F).indices_set {
            self.set_bit(index)
        }
    }

    pub fn has<H: AsRef<[u8]>>(&self, hash: &H) -> bool {
        for index in Self::build_expected(hash).folded(F).indices_set {
            if !self.test_bit(index) {
                return false;
            }
        }
        return true;
    }

    fn build_expected<H: AsRef<[u8]>>(hash: &H) -> SparseArray {
        // sparse array
        let mut expected = SparseArray::new_with_capacity(K);

        for seed in 0..K {
            let index = xxh3_64_with_seed(hash.as_ref(), seed as u64) as usize % (S * 8 << F);
            expected.set_bit(index);
        }

        return expected;
    }

    fn set_bit(&mut self, index: usize) {
        let byte_index = index / 8;
        let bit_index = index % 8;
        self.bytes[byte_index] |= 1u8 << bit_index;
    }

    fn test_bit(&self, index: usize) -> bool {
        let byte_index = index / 8;
        let bit_index = index % 8;
        (self.bytes[byte_index] & (1u8 << bit_index)) != 0
    }
}

struct SparseArray {
    indices_set: Vec<usize>,
}

impl SparseArray {
    fn new_with_capacity(capacity: usize) -> Self {
        Self {
            indices_set: Vec::with_capacity(capacity),
        }
    }

    fn set_bit(&mut self, index: usize) {
        if let Some(_) = self
            .indices_set
            .iter()
            .find(|index_set| **index_set == index)
        {
            return;
        }
        self.indices_set.push(index)
    }

    fn flip_bit(&mut self, index: usize) {
        if let Some((index_index, _)) = self
            .indices_set
            .iter()
            .enumerate()
            .find(|(_, index_set)| **index_set == index)
        {
            self.indices_set.remove(index_index);
        } else {
            self.indices_set.push(index);
        }
    }

    fn test_bit(&self, index: usize) -> bool {
        return self
            .indices_set
            .iter()
            .find(|index_set| **index_set == index)
            .is_some();
    }

    fn folded(&self, times: usize) -> SparseArray {
        let mut result = Self::new_with_capacity(self.indices_set.len());
        for index in self.indices_set.iter() {
            result.flip_bit(index >> times);
        }
        result
    }
}

#[test]
fn test_folded() {
    let mut bloom = Folded::<1, 128, 30>::new();
    bloom.insert(b"Hello, World");
    assert!(bloom.has(b"Hello, World"));
    assert!(!bloom.has(b"Test"));
}
