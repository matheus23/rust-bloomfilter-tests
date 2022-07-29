mod folded;

use std::{io::Write, iter, time::Instant};

use blake3;
use folded::Folded;
use rand::RngCore;
use xxhash_rust::xxh3::{self};

// M bytes (m = M * 8) and K hash functions
#[derive(Clone)]
struct Bloom<const M: usize, const K: usize> {
    bytes: [u8; M],
}

// Indices in a bloom filter based on XXH3

struct BloomIndicesXXH3<'a, const M: usize> {
    element: &'a [u8],
    seed: u64,
}

impl<'a, const M: usize> From<&'a [u8]> for BloomIndicesXXH3<'a, M> {
    fn from(element: &'a [u8]) -> Self {
        Self { element, seed: 0 }
    }
}

impl<'a, const M: usize> Iterator for BloomIndicesXXH3<'a, M> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let hash = xxh3::xxh3_64_with_seed(self.element, self.seed) as usize;
        self.seed += 1;
        Some(hash % (M * 8))
    }
}

struct BloomIndicesXXH3RejectionSampling<'a, const M: usize> {
    element: &'a [u8],
    bitmask: usize,
    seed: u64,
}

impl<'a, const M: usize> From<&'a [u8]> for BloomIndicesXXH3RejectionSampling<'a, M> {
    fn from(element: &'a [u8]) -> Self {
        let max = M * 8;
        let bitmask = (if max.count_ones() == 1 {
            max
        } else {
            max.next_power_of_two()
        } - 1);
        Self {
            element,
            bitmask,
            seed: 0,
        }
    }
}

impl<'a, const M: usize> Iterator for BloomIndicesXXH3RejectionSampling<'a, M> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let mut index = (xxh3::xxh3_64_with_seed(self.element, self.seed) as usize) & self.bitmask;

        // Try to generate something within bounds
        while index >= M * 8 {
            self.seed += 1;
            index = (xxh3::xxh3_64_with_seed(self.element, self.seed) as usize) & self.bitmask;
        }

        self.seed += 1;
        Some(index)
    }
}

struct BloomIndicesDistinctXXH3<'a, const M: usize> {
    used_nums: [bool; M],
    index_iterator: BloomIndicesXXH3<'a, M>,
}

impl<'a, const M: usize> Iterator for BloomIndicesDistinctXXH3<'a, M> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        macro_rules! otry {
            ($e:expr) => {
                match $e {
                    Some(e) => e,
                    None => return None,
                }
            };
        }
        let mut index = otry!(self.index_iterator.next());
        loop {
            let was_used = self.used_nums[index];
            self.used_nums[index] = true;

            if !was_used {
                return Some(index);
            }

            index = otry!(self.index_iterator.next());
        }
    }
}

struct BloomIndicesBlake3<const M: usize> {
    output_reader: blake3::OutputReader,
}

impl<const M: usize> From<&[u8]> for BloomIndicesBlake3<M> {
    fn from(element: &[u8]) -> Self {
        Self {
            output_reader: blake3::Hasher::new().update(element).finalize_xof(),
        }
    }
}

impl<const M: usize> Iterator for BloomIndicesBlake3<M> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = [0u8; 8];
        self.output_reader.fill(&mut buf);
        let yld = usize::from_le_bytes(buf);
        Some(yld % (M * 8))
    }
}

impl<const M: usize, const K: usize> Bloom<M, K> {
    pub fn new() -> Self {
        Self { bytes: [0; M] }
    }

    pub fn add(&mut self, element: &[u8]) {
        for index in BloomIndicesXXH3RejectionSampling::<M>::from(element).take(K) {
            self.set_bit(index);
        }
    }

    pub fn has(&self, element: &[u8]) -> bool {
        for index in BloomIndicesXXH3RejectionSampling::<M>::from(element).take(K) {
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

    pub fn saturate(&mut self) {
        let mut xof = blake3::Hasher::new_derive_key("nyberg accumulator saturation")
            .update(&self.bytes)
            .finalize_xof();
        let mut buffer = [0u8; 32];

        loop {
            xof.fill(&mut buffer);
            let mut cloned = self.clone();
            cloned.add(&buffer);
            if cloned.count_ones() > 1019 {
                return;
            } else {
                self.bytes = cloned.bytes;
            }
        }
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

fn fill_deterministic<const M: usize, const K: usize>(
    seed: &str,
    elements: u32,
    bloom: &mut Bloom<M, K>,
) {
    let mut output_reader = blake3::Hasher::new_derive_key(seed)
        .update(b"Hello, world!")
        .finalize_xof();

    let mut buffer = [0u8; 32];

    for _ in 0..elements {
        output_reader.fill(&mut buffer);
        bloom.add(&buffer);
    }
}

fn fill_random<const M: usize, const K: usize>(elements: u32, bloom: &mut Bloom<M, K>) {
    for _ in 0..elements {
        let mut randoms = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut randoms);
        bloom.add(&randoms);
    }
}

fn print_test_progress(i: u64, tests: u64) {
    if i % 1000 == 0 {
        print!("\r{:>5}/{tests}            ", i);
        std::io::stdout().flush().unwrap();
    }
}

fn test_avg_bits(prefill: u32, tests: u64) {
    let mut sum = 0;
    for i in 0..tests {
        let mut bloom: Bloom<256, 30> = Bloom::new();
        fill_random(prefill, &mut bloom);

        sum += bloom.count_ones();
        print_test_progress(i, tests);
    }

    println!("\n{}", (sum as f64) / (tests as f64));
}

const TESTS: usize = 100_000;
fn test_avg_saturation_bits() {
    let mut histo = [0u64; 256];

    const BYTES: usize = 32 * TESTS;

    let mut rando = [0u8; BYTES];
    rand::thread_rng().fill_bytes(&mut rando);

    let before = Instant::now();
    for i in 0..TESTS {
        let mut bloom: Bloom<256, 30> = Bloom::new();

        bloom.add(&rando[i * 32..(i + 1) * 32]);
        bloom.saturate();

        histo[bloom.count_ones() as usize - 896] += 1;
        print_test_progress(i as u64, TESTS as u64);
    }
    let after = Instant::now();

    println!("\nbits;amount");
    for (i, v) in histo.iter().enumerate() {
        println!("{};{v}", i + 896);
    }
    println!("{}", after.duration_since(before).as_millis());
}

fn test_false_positive_rate(prefill: u32, tests: u64) {
    let mut bloom: Bloom<256, 30> = Bloom::new();

    fill_deterministic("Bloom filter prefill", prefill, &mut bloom);

    println!("{}", bloom.count_ones());
    let before = Instant::now();

    let mut false_positive_count = 0;
    for i in 0..tests {
        if bloom.has(&i.to_le_bytes()) {
            false_positive_count += 1;
        }
        if i % 100_000 == 0 {
            print_test_progress(i, tests);
        }
    }

    let after = Instant::now();
    println!(
        "{false_positive_count}/{tests} {}ms",
        after.duration_since(before).as_millis()
    );
}

fn main() {
    // test_false_positive_rate(47, 1_000_000_000);
    // test_avg_saturation_bits();
    test_folded_rates();
}

#[test]
fn test_bitavg() {
    test_avg_bits(47, 100_000);
}

#[test]
fn test_xof() {
    use sha3;
    use sha3::digest::{ExtendableOutput, Update, XofReader};

    let mut hasher = sha3::Shake256::default();
    hasher.update(b"Hello, World!");
    let mut xof = hasher.finalize_xof();
    let buffer = &mut [0u8; 10];
    xof.read(buffer);

    println!("{:02x?}", buffer);
}

// #[test]
// fn test_sha3_hashing_speed() {
//     let before = Instant::now();
//     use sha3::Digest;

//     let mut hasher = sha3::Sha3_256::default();
//     hasher.update(b"Hello, World!");
//     let mut hash: [u8; 32] = hasher.finalize_reset().into();

//     for _ in 0..100_000_000 {
//         hasher.update(hash);
//         hash = hasher.finalize_reset().into();
//     }

//     let after = Instant::now();
//     println!(
//         "{} {}",
//         after.duration_since(before).as_millis(),
//         hex::encode(hash)
//     );
// }

#[test]
fn test_xxh3_hashing_speed() {
    let before = Instant::now();

    let mut hash: u64 = 1000;

    for _ in 0..100_000_000 {
        hash = xxh3::xxh3_64(&hash.to_le_bytes());
    }

    let after = Instant::now();
    println!("{} {}", after.duration_since(before).as_millis(), hash);
}

struct Blake3XOF {
    output_reader: blake3::OutputReader,
}

impl Blake3XOF {
    fn new<D: AsRef<[u8]>>(data: &D) -> Self {
        Self {
            output_reader: blake3::Hasher::new().update(data.as_ref()).finalize_xof(),
        }
    }
}

impl Iterator for Blake3XOF {
    type Item = [u8; 32];

    fn next(&mut self) -> Option<Self::Item> {
        let mut bytes = [0u8; 32];
        self.output_reader.fill(&mut bytes);
        Some(bytes)
    }
}

const M: usize = 262_144; // original bloom filter bits
const K: usize = 18; // num of hash functions
const F: usize = 0; // num of folds
const S: usize = (M / 8) >> F; // byte size of folded filter

fn test_folded_rates() {
    let min = 4000;
    let max = 30000;
    let step_size = 100;

    for n_fac in (min / step_size)..(max / step_size + 1) {
        let n = step_size * n_fac;

        let mut filter: Folded<F, S, K> = Folded::new();
        for item in Blake3XOF::new(b"In the filter").take(n) {
            filter.insert(&item);
        }

        let mut false_negative_count = 0;
        for item_in_filter in Blake3XOF::new(b"In the filter").take(n) {
            if !filter.has(&item_in_filter) {
                false_negative_count += 1;
            }
        }

        let mut false_positive_count = 0;
        for not_in_filter in Blake3XOF::new(b"Not in the filter").take(1_000_000) {
            if filter.has(&not_in_filter) {
                false_positive_count += 1;
            }
        }

        println!("{n}, {false_negative_count}, {false_positive_count}")
    }
}

#[test]
fn test_vectors() {
    let mut bloom: Bloom<125, 4> = Bloom::new();
    bloom.add(b"one");
    // bloom.add(b"two");
    bloom.add(b"three");
    assert_eq!(hex::encode(bloom.bytes), "0000000000000000000000000000000000000000000000000000000000000000000000000000100000000000004000000000000001000000000000000000000000000400004000000000000000800000000000000000000000000000000000000000000000000000000000000000000020000000000000000000000400");
}

#[test]
fn test_sth() {
    // let decoded: Vec<u8> = hex::decode("0000000000000000000000000000000000000000000000000000000000000000000000000000100000000000004000000000000001000000000000000000000000000400004000000000000000800000000000000000000000000000000000000000000000000000000000000000000020000000000000000000000400").unwrap();
    let decoded: Vec<u8> = hex::decode("0000000000000000000000000000000000000000000400000000000000000000000000000000100000000000004000000008000001000000000000000000002000000400004000000000000000800000000000000000000000000000000000000000000000000000000000002000000020000000000000000000000400").unwrap();
    let mut count = 0;
    for u in decoded {
        count += u.count_ones();
    }
    println!("{count}");
}

#[test]
fn test_indices() {
    println!("indices for 'one':");
    for index in BloomIndicesXXH3RejectionSampling::<125>::from(b"one" as &[u8]).take(4) {
        println!("{index}");
    }
    println!("indices for 'two':");
    for index in BloomIndicesXXH3RejectionSampling::<125>::from(b"two" as &[u8]).take(4) {
        println!("{index}");
    }
    println!("indices for 'three':");
    for index in BloomIndicesXXH3RejectionSampling::<125>::from(b"three" as &[u8]).take(4) {
        println!("{index}");
    }
}
