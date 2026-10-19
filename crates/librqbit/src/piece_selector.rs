//! Lets the embedder decide which pieces a torrent requests first.

use librqbit_core::lengths::ValidPieceIndex;

/// Decides which pieces a torrent requests first, in place of the readahead of its streams.
///
/// Set it with `AddTorrentOptions::piece_selector`. After its choices change, call
/// [`ManagedTorrent::reselect_pieces`](crate::ManagedTorrent::reselect_pieces) so that idle
/// peers look again.
pub trait PieceSelector: Send + Sync {
    /// Calls `f` with the pieces to request before any other, most urgent first, until `f`
    /// returns false. It's called under the torrent's lock whenever a peer looks for a piece, so
    /// it must be quick and must not call back into the torrent.
    fn priority_pieces(&self, f: &mut dyn FnMut(ValidPieceIndex) -> bool);

    /// Whether to request pieces outside [`priority_pieces`](Self::priority_pieces), in the
    /// order of file priorities, when none of those can be requested.
    fn download_other_pieces(&self) -> bool {
        true
    }
}
