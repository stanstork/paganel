use crate::{RowHashIter, error::StateStoreError};
use model::integrity::{
    algorithm::HashAlgorithm, merkle::MerkleAccumulator, row_key::KeyedRowHash,
};

/// Leaves per parallel fold block.
const FOLD_BLOCK_LEAVES: usize = 65_536;

/// Upper bound on fold workers.
const MAX_FOLD_WORKERS: usize = 8;

/// Fold a row-hash stream into `(leaf count, root)`.
pub fn fold_root(
    mut iter: RowHashIter,
    algorithm: HashAlgorithm,
) -> Result<(u64, [u8; 32]), StateStoreError> {
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .min(MAX_FOLD_WORKERS);

    let mut root = MerkleAccumulator::new(algorithm);
    let mut drained = false;

    // Pre-allocate blocks to avoid massive allocations in the while loop.
    let mut idle_blocks = vec![Vec::with_capacity(FOLD_BLOCK_LEAVES); workers];

    while !drained {
        let mut active_blocks = Vec::with_capacity(workers);

        // Fill up to `workers` blocks from the idle pool
        while let Some(mut block) = idle_blocks.pop() {
            block.clear();
            for entry in iter.by_ref().take(FOLD_BLOCK_LEAVES) {
                block.push(entry?);
            }

            let partial = block.len() < FOLD_BLOCK_LEAVES;
            if !block.is_empty() {
                active_blocks.push(block);
            }
            if partial {
                drained = true;
                break;
            }
        }

        if active_blocks.is_empty() {
            break;
        }

        let partial_results: Vec<(MerkleAccumulator, Vec<KeyedRowHash>)> =
            std::thread::scope(|scope| {
                let handles: Vec<_> = active_blocks
                    .into_iter()
                    .map(|block| {
                        scope.spawn(move || {
                            let mut acc = MerkleAccumulator::new(algorithm);
                            for entry in &block {
                                acc.push_row(&entry.key, &entry.hash);
                            }
                            (acc, block) // Return the buffer so we can recycle it
                        })
                    })
                    .collect();

                handles
                    .into_iter()
                    .map(|handle| handle.join())
                    .collect::<Result<Vec<_>, _>>()
            })
            .map_err(|_| StateStoreError::Storage("integrity fold worker panicked".into()))?;

        // Merge results and push recycled blocks back into the idle pool
        for (partial_acc, recycled_block) in partial_results {
            root.merge(partial_acc);
            idle_blocks.push(recycled_block);
        }
    }

    Ok((root.leaf_count(), root.finish()))
}

#[cfg(test)]
mod tests {
    use model::integrity::{
        algorithm::HashAlgorithm, merkle::MerkleAccumulator, row_key::KeyedRowHash,
    };

    use crate::{
        RowHashIter,
        error::StateStoreError,
        log::fold::{FOLD_BLOCK_LEAVES, fold_root},
    };

    /// Folding in parallel must produce exactly the tree a single pass builds.
    #[test]
    fn parallel_fold_matches_a_single_pass() {
        let algorithm = HashAlgorithm::Sha256;

        // Several full blocks plus a partial one, so both paths are exercised.
        let rows: Vec<KeyedRowHash> = (0..FOLD_BLOCK_LEAVES * 3 + 7)
            .map(|i| KeyedRowHash {
                key: (i as u64).to_be_bytes().to_vec(),
                hash: [(i % 251) as u8; 32],
            })
            .collect();

        let mut sequential = MerkleAccumulator::new(algorithm);
        for row in &rows {
            sequential.push_row(&row.key, &row.hash);
        }
        let expected = (sequential.leaf_count(), sequential.finish());

        let iter: RowHashIter = Box::new(rows.into_iter().map(Ok));
        let actual = fold_root(iter, algorithm).expect("fold");

        assert_eq!(actual, expected);
    }

    /// The leaf count and root of an empty table must still be well defined.
    #[test]
    fn parallel_fold_handles_an_empty_stream() {
        let algorithm = HashAlgorithm::Sha256;
        let expected = (0, MerkleAccumulator::new(algorithm).finish());

        let iter: RowHashIter = Box::new(std::iter::empty());
        assert_eq!(fold_root(iter, algorithm).expect("fold"), expected);
    }

    /// A read failure mid-stream must surface, not truncate the tree.
    #[test]
    fn parallel_fold_propagates_a_read_error() {
        let rows = (0..10).map(|i| {
            Ok(KeyedRowHash {
                key: vec![i],
                hash: [i; 32],
            })
        });
        let failing = rows.chain(std::iter::once(Err(StateStoreError::Storage(
            "boom".to_string(),
        ))));

        let iter: RowHashIter = Box::new(failing);
        assert!(fold_root(iter, HashAlgorithm::Sha256).is_err());
    }
}
