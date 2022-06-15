use std::time::Instant;

use blake3;
use xxhash_rust::xxh3;
// use xxhash_rust::xxh32;
// use xxhash_rust::xxh64;

struct Bloom<const N: usize> {
    bytes: [u8; N],
    k_hashes: u64,
}

const LUT: [u8; 8] = [
    0b0000_0001,
    0b0000_0010,
    0b0000_0100,
    0b0000_1000,
    0b0001_0000,
    0b0010_0000,
    0b0100_0000,
    0b1000_0000,
];

struct EnhancedDoubleHash {
    x: u32,
    y: u32,
    n: u32,
}

impl EnhancedDoubleHash {
    fn new(hash1: u32, hash2: u32) -> Self {
        Self {
            x: hash1,
            y: hash2,
            n: 1,
        }
    }
}

impl Iterator for EnhancedDoubleHash {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        self.x = self.x.wrapping_add(self.y);
        self.y = self.y.wrapping_add(self.n);
        self.n += 1;

        Some(self.x)
    }
}

impl<const N: usize> Bloom<N> {
    pub fn new(k_hashes: u64) -> Self {
        Self {
            bytes: [0; N],
            k_hashes,
        }
    }

    #[cfg(not(indices = "edh"))]
    pub fn indices<'a>(&self, element: &'a [u8]) -> impl Iterator<Item = usize> + 'a {
        (0..self.k_hashes).map(|seed| {
            let hash = xxh3::xxh3_64_with_seed(element, seed) as usize;
            // let hash = xxh64::xxh64(element, seed) as usize;
            // let hash = xxh32::xxh32(element, seed as u32) as usize;
            hash % (N * 8)
        })
    }

    #[cfg(indices = "edh")]
    pub fn indices<'a>(&self, element: &'a [u8]) -> impl Iterator<Item = usize> + 'a {
        let hash = xxh3::xxh3_64(element);
        let hash1 = (hash >> 32) as u32;
        let hash2 = hash as u32;
        EnhancedDoubleHash::new(hash1, hash2)
            .take(self.k_hashes as usize)
            .map(|n| (n as usize) % (N * 8))
    }

    pub fn add(&mut self, element: &[u8]) {
        for index in self.indices(element) {
            self.set_bit(index);
        }
    }

    pub fn has(&self, element: &[u8]) -> bool {
        for index in self.indices(element) {
            if !self.test_bit(index) {
                return false;
            }
        }
        return true;
    }

    pub fn count_ones(&self) -> u32 {
        let mut ones = 0;
        for n in self.bytes.iter() {
            ones += n.count_ones();
        }
        ones
    }

    pub fn as_hex(&self) -> String {
        hex::encode(self.bytes)
    }

    fn set_bit(&mut self, index: usize) {
        let byte_index = index / 8;
        let bit_index = index % 8;
        self.bytes[byte_index] |= LUT[bit_index];
    }

    fn test_bit(&self, index: usize) -> bool {
        let byte_index = index / 8;
        let bit_index = index % 8;
        (self.bytes[byte_index] & LUT[bit_index]) != 0
    }
}

const PREFILL: u32 = 47;
const TESTS: u64 = 10_000_000_000;

fn main() {
    let mut bloom: Bloom<256> = Bloom::new(30);

    let mut hash = blake3::hash(b"Seed?");
    let mut hash_bytes: &[u8; 32] = hash.as_bytes();

    for _ in 0..PREFILL {
        bloom.add(hash_bytes);

        hash = blake3::hash(hash_bytes);
        hash_bytes = hash.as_bytes();
    }

    // now the bloom filter is filled with 47 PRNG items

    println!("{}", bloom.count_ones());
    let before = Instant::now();

    let mut false_positive_count = 0;
    for i in 0..TESTS {
        if bloom.has(&i.to_le_bytes()) {
            false_positive_count += 1;
        }
    }

    let after = Instant::now();
    println!(
        "{false_positive_count}/{TESTS} {}ms",
        after.duration_since(before).as_millis()
    );
}
