use std::{collections::HashSet, sync::Arc};

use librqbit_core::lengths::ValidPieceIndex;
use tracing::{debug, warn};

use crate::{
    chunk_tracker::{ChunkTracker, HaveNeededSelected},
    type_aliases::FileStorage,
};

use super::{ManagedTorrentShared, TorrentMetadata, streaming::TorrentStreams};

pub struct TorrentStatePaused {
    pub(crate) shared: Arc<ManagedTorrentShared>,
    pub(crate) metadata: Arc<TorrentMetadata>,
    pub(crate) files: FileStorage,
    pub(crate) chunk_tracker: ChunkTracker,
    pub(crate) streams: Arc<TorrentStreams>,
}

impl TorrentStatePaused {
    pub(crate) fn update_only_files(&mut self, only_files: &HashSet<usize>) -> anyhow::Result<()> {
        self.chunk_tracker
            .update_only_files(&self.metadata.file_infos, only_files)?;
        Ok(())
    }

    pub(crate) fn hns(&self) -> &HaveNeededSelected {
        self.chunk_tracker.get_hns()
    }

    // The caller holds the torrent's lock exclusively, so no reader is between its
    // completion check and its read.
    pub(crate) fn invalidate_pieces(&mut self, pieces: &[ValidPieceIndex]) -> usize {
        let mut invalidated = 0;
        for &id in pieces {
            if !self.chunk_tracker.is_piece_have(id) {
                continue;
            }
            if let Err(e) = self.files.on_piece_invalidated(id) {
                debug!(
                    ?id,
                    "file storage kept the piece in on_piece_invalidated(): {e:#}"
                );
                continue;
            }
            self.chunk_tracker
                .mark_piece_missing(id, &self.metadata.file_infos);
            invalidated += 1;
        }
        if invalidated > 0
            && let Err(e) = self.chunk_tracker.get_have_pieces_mut().flush(true)
        {
            warn!(id=?self.shared.id, info_hash = ?self.shared.info_hash, "error flushing bitfield: {e:#}");
        }
        invalidated
    }
}
