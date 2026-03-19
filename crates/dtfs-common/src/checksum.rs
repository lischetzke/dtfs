/// BLAKE3-based checksum utilities for data integrity verification.

/// Compute BLAKE3 hash of a byte slice.
pub fn compute(data: &[u8]) -> [u8; 32] {
    *blake3::hash(data).as_bytes()
}

/// Compute BLAKE3 hash incrementally using a hasher.
pub struct IncrementalHasher {
    hasher: blake3::Hasher,
}

impl IncrementalHasher {
    pub fn new() -> Self {
        Self {
            hasher: blake3::Hasher::new(),
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    pub fn finalize(self) -> [u8; 32] {
        *self.hasher.finalize().as_bytes()
    }
}

impl Default for IncrementalHasher {
    fn default() -> Self {
        Self::new()
    }
}

/// Verify that data matches an expected checksum.
pub fn verify(data: &[u8], expected: &[u8; 32]) -> bool {
    let actual = compute(data);
    actual == *expected
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_and_verify() {
        let data = b"hello world";
        let hash = compute(data);
        assert!(verify(data, &hash));
        assert!(!verify(b"wrong data", &hash));
    }

    #[test]
    fn test_incremental_hasher() {
        let data = b"hello world";
        let direct = compute(data);

        let mut hasher = IncrementalHasher::new();
        hasher.update(b"hello ");
        hasher.update(b"world");
        let incremental = hasher.finalize();

        assert_eq!(direct, incremental);
    }

    #[test]
    fn test_deterministic() {
        let data = b"test data for checksumming";
        let hash1 = compute(data);
        let hash2 = compute(data);
        assert_eq!(hash1, hash2);
    }
}
