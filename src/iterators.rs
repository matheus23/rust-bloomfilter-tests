use std::mem;

use xxhash_rust::xxh3;

macro_rules! otry {
    ($e:expr) => {
        match $e {
            Some(e) => e,
            None => return None,
        }
    };
}

// rejection sampling
pub struct RejectionSampling<I, O> {
    iter: I,
    max: O,
}

impl<I: Iterator<Item = O>, O: Ord> RejectionSampling<I, O> {
    pub fn accept_smaller(iter: I, max: O) -> Self {
        Self { iter, max }
    }
}

impl<I: Iterator<Item = O>, O: Ord> Iterator for RejectionSampling<I, O> {
    type Item = O;

    fn next(&mut self) -> Option<Self::Item> {
        let mut val = otry!(self.iter.next());

        // Try to generate something within bounds
        while val >= self.max {
            val = otry!(self.iter.next());
        }

        Some(val)
    }
}

// skip duplicates
pub struct DistinctSampling<I, V> {
    iter: I,
    used_values: Vec<V>,
}

impl<I: Iterator<Item = V>, V: Eq + Copy> DistinctSampling<I, V> {
    pub fn distinct(iter: I) -> Self {
        Self {
            iter,
            used_values: Vec::new(),
        }
    }
}

impl<I: Iterator<Item = V>, V: Eq + Copy> Iterator for DistinctSampling<I, V> {
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        let mut val = otry!(self.iter.next());
        while self.used_values.iter().find(|v| **v == val).is_some() {
            val = otry!(self.iter.next());
        }
        self.used_values.push(val);
        Some(val)
    }
}

// take n bits at a time
pub struct YieldBits<I> {
    iter: I,
    bits: usize,
    last: Option<usize>,
    bits_used: usize,
}

impl<I: Iterator<Item = usize>> YieldBits<I> {
    pub fn yield_bits(iter: I, bits_at_a_time: usize) -> Self {
        Self {
            iter,
            bits: bits_at_a_time,
            last: None,
            bits_used: 0,
        }
    }
}

impl<I: Iterator<Item = usize>> Iterator for YieldBits<I> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let last = if self.bits_used + self.bits > mem::size_of::<usize>() * 8 {
            self.bits_used = 0;
            otry!(self.iter.next())
        } else {
            match self.last {
                Some(l) => l,
                None => otry!(self.iter.next()),
            }
        };
        self.last = Some(last);

        let result = (last >> self.bits_used) & ((1 << self.bits) - 1);
        self.bits_used += self.bits;
        Some(result)
    }
}

// XXH3 extendable output function
pub struct XXH3XOF<'a> {
    element: &'a [u8],
    seed: u64,
}

impl<'a> From<&'a [u8]> for XXH3XOF<'a> {
    fn from(element: &'a [u8]) -> Self {
        Self { element, seed: 0 }
    }
}

impl<'a> Iterator for XXH3XOF<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        let hash = xxh3::xxh3_64_with_seed(self.element, self.seed);
        self.seed += 1;
        Some(hash)
    }
}

// blake3 extendable output function that outputs u64s at a time
pub struct Blake3XOF {
    output_reader: blake3::OutputReader,
}

impl From<&[u8]> for Blake3XOF {
    fn from(element: &[u8]) -> Self {
        Self {
            output_reader: blake3::Hasher::new().update(element).finalize_xof(),
        }
    }
}

impl Iterator for Blake3XOF {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = [0u8; 8];
        self.output_reader.fill(&mut buf);
        let yld = u64::from_le_bytes(buf);
        Some(yld)
    }
}

#[test]
fn test_yield_bits() {
    for val in XXH3XOF::from(b"Hello, World!" as &[u8]).take(2) {
        println!("{:x}", val);
    }
    for val in YieldBits::yield_bits(
        XXH3XOF::from(b"Hello, World!" as &[u8])
            .take(2)
            .map(|u| u as usize),
        8,
    ) {
        println!("{:x}", val);
    }
}
