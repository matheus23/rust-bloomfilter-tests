mod folded;
mod iterators;

use blake3;
use folded::Folded;
use iterators::*;
use rand::RngCore;
use std::{io::Write, time::Instant};
use xxhash_rust::xxh3;

// M bytes (m = M * 8) and K hash functions
#[derive(Clone)]
struct Bloom<const M: usize, const K: usize> {
    bytes: [u8; M],
}

impl<const M: usize, const K: usize> Bloom<M, K> {
    pub fn new() -> Self {
        Self { bytes: [0; M] }
    }

    pub fn add(&mut self, element: &[u8]) {
        for index in bloom_indices_for_element(element, M * 8, K) {
            self.set_bit(index);
        }
    }

    pub fn has(&self, element: &[u8]) -> bool {
        for index in bloom_indices_for_element(element, M * 8, K) {
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

fn bloom_indices_for_element(
    element: &[u8],
    max: usize,
    k: usize,
) -> impl Iterator<Item = usize> + '_ {
    let mut next_pow_of2 = if max.count_ones() == 1 {
        max
    } else {
        max.next_power_of_two()
    };
    let mut pow = 1;
    while next_pow_of2 != 0 {
        next_pow_of2 >>= 1;
        pow += 1;
    }
    RejectionSampling::accept_smaller(
        YieldBits::yield_bits(XXH3XOF::from(element).map(|u| u as usize), pow),
        max,
    )
    .take(k)
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
    test_indices_for("one", 1000, 4);
    test_indices_for("two", 1000, 4);
    test_indices_for("three", 1000, 4);
    test_indices_for("ducks", 10, 3);
    test_indices_for("chickens", 10, 3);
    test_indices_for("goats", 10, 3);
}

fn test_indices_for(s: &str, m: usize, k: usize) {
    println!("indices for '{s}':");
    for index in bloom_indices_for_element(s.as_bytes(), m, k) {
        println!("{index}");
    }
}
