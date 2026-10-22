//! Coordinates piece download state by wrapping ChunkTracker with inflight piece tracking.
//!
//! This module provides the [`PieceTracker`] type which encapsulates the relationship between
//! queued pieces (in ChunkTracker) and in-flight pieces (being downloaded by a peer).
//!
//! The key invariant maintained is: a piece is in exactly one state at any time:
//! - HAVE (completed)
//! - QUEUED (available to download)
//! - IN_FLIGHT (currently being downloaded)
//! - NOT_NEEDED (not selected for download)

use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use buffers::ByteBuf;
use librqbit_core::lengths::ValidPieceIndex;
use peer_binary_protocol::Piece;

use crate::{
    chunk_tracker::{ChunkMarkingResult, ChunkTracker},
    file_info::FileInfo,
    type_aliases::{FileInfos, FilePriorities, PeerHandle},
};

/// The most peers, its owner included, that fetch one shared piece.
const MAX_SHARED_PARTICIPANTS: usize = 3;

/// The most participants that request one chunk of a shared piece: its first
/// claimant and one duplicate.
const CLAIMS_PER_CHUNK: usize = 2;

/// A participant requests duplicates only while it holds fewer requests in
/// the piece than this, so duplicates stay a bounded tail of the piece.
const DUPLICATING_BELOW: usize = 2;

/// The most requests one participant holds in a shared piece, 256 KiB of
/// 16 KiB chunks: enough to keep a slow link busy, few enough that the first
/// peer to reserve a large piece leaves most of its chunks to the others.
const SHARED_PIPELINE_CHUNKS: usize = 16;

/// What a participant of an in-flight piece requests next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkClaim {
    /// This chunk, and whether another participant requested it too.
    Chunk { index: usize, duplicate: bool },
    /// Nothing until one of the participant's requests in the piece is
    /// answered: it holds as many as a shared piece allows, and chunks are
    /// left for the others.
    Full,
    /// Nothing more: the participant is done with the piece.
    Done,
}

/// Tracks a piece currently being downloaded.
#[derive(Debug, Clone)]
pub struct InflightPiece {
    pub peer: PeerHandle,
    pub started: Instant,
    /// Peers other than the owner fetching chunks of a shared piece.
    pub joiners: Vec<PeerHandle>,
    /// The participants that requested each chunk, until it arrives.
    claims: Vec<[Option<Claim>; CLAIMS_PER_CHUNK]>,
    /// The order of the next claim, so the most recent request is known.
    next_order: u32,
    /// Whether the piece is shared, so each participant holds few of its
    /// requests at once.
    shared: bool,
}

/// A participant's request for a chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Claim {
    peer: PeerHandle,
    order: u32,
}

impl InflightPiece {
    fn new(peer: PeerHandle, chunks: u32) -> Self {
        Self {
            peer,
            started: Instant::now(),
            joiners: Vec::new(),
            claims: vec![[None; CLAIMS_PER_CHUNK]; chunks as usize],
            next_order: 0,
            shared: false,
        }
    }

    /// Whether `peer` owns or has joined this piece.
    pub fn is_participant(&self, peer: PeerHandle) -> bool {
        self.peer == peer || self.joiners.contains(&peer)
    }

    /// The owner and the joiners.
    pub fn participants(&self) -> impl Iterator<Item = PeerHandle> + '_ {
        std::iter::once(self.peer).chain(self.joiners.iter().copied())
    }

    /// Frees the chunks `peer` requested, so another participant can.
    fn release_claims_of(&mut self, peer: PeerHandle) {
        for claim in self.claims.iter_mut().flatten() {
            if claim.is_some_and(|claim| claim.peer == peer) {
                *claim = None;
            }
        }
    }

    fn release_all_claims(&mut self) {
        self.claims.fill([None; CLAIMS_PER_CHUNK]);
    }

    /// Removes `peer` from the participants, handing ownership to the first
    /// joiner if `peer` owned the piece. Returns false if `peer` owned it
    /// alone, which leaves the piece unchanged.
    fn leave(&mut self, peer: PeerHandle) -> bool {
        if self.peer == peer {
            if self.joiners.is_empty() {
                return false;
            }
            self.peer = self.joiners.remove(0);
        } else {
            self.joiners.retain(|joiner| *joiner != peer);
        }
        self.release_claims_of(peer);
        true
    }

    fn claimed_by(&self, chunk: usize, peer: PeerHandle) -> bool {
        self.claims[chunk]
            .iter()
            .flatten()
            .any(|claim| claim.peer == peer)
    }

    /// What `peer` would request next: the first chunk not downloaded and
    /// not requested, from the last chunk backwards for a joiner, while it
    /// holds fewer than [`SHARED_PIPELINE_CHUNKS`] requests in a shared piece.
    /// Once every such chunk is requested, and while `peer` holds few
    /// requests here, a duplicate of the most recent request of another
    /// participant, since it is likely the furthest from arriving; whichever
    /// copy arrives first completes the chunk.
    fn next_claim(
        &self,
        chunks: &ChunkTracker,
        piece: ValidPieceIndex,
        peer: PeerHandle,
    ) -> ChunkClaim {
        let count = self.claims.len();
        let missing = |chunk: &usize| !chunks.is_chunk_downloaded(piece, *chunk);
        let free =
            |chunk: &usize| missing(chunk) && self.claims[*chunk].iter().all(Option::is_none);
        let held = (0..count)
            .filter(|chunk| missing(chunk) && self.claimed_by(*chunk, peer))
            .count();
        let first_free = if peer == self.peer {
            (0..count).find(free)
        } else {
            (0..count).rev().find(free)
        };
        if let Some(index) = first_free {
            if self.shared && held >= SHARED_PIPELINE_CHUNKS {
                return ChunkClaim::Full;
            }
            return ChunkClaim::Chunk {
                index,
                duplicate: false,
            };
        }
        if held >= DUPLICATING_BELOW {
            return ChunkClaim::Done;
        }
        (0..count)
            .filter(|chunk| {
                missing(chunk)
                    && !self.claimed_by(*chunk, peer)
                    && self.claims[*chunk].contains(&None)
            })
            .max_by_key(|chunk| {
                self.claims[*chunk]
                    .iter()
                    .flatten()
                    .map(|claim| claim.order)
                    .max()
            })
            .map_or(ChunkClaim::Done, |index| ChunkClaim::Chunk {
                index,
                duplicate: true,
            })
    }

    /// Records that `peer` requests `chunk`.
    fn claim(&mut self, chunk: usize, peer: PeerHandle) {
        let order = self.next_order;
        if let Some(slot) = self.claims[chunk].iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(Claim { peer, order });
            self.next_order += 1;
        }
    }
}

/// Result of attempting to acquire a piece.
#[derive(Debug)]
pub enum AcquireResult {
    /// A new piece was reserved from the queue.
    Reserved(ValidPieceIndex),
    /// A piece was stolen from a slower peer.
    Stolen {
        piece: ValidPieceIndex,
        from_peer: PeerHandle,
    },
    /// The peer joined a shared piece another peer is fetching; it requests
    /// the piece's chunks from the last one backwards.
    Joined(ValidPieceIndex),
    /// No pieces are available for this peer.
    NoneAvailable,
}

/// Parameters for acquiring a piece.
pub struct AcquireRequest<'a, I, P, S, R>
where
    I: Iterator<Item = ValidPieceIndex>,
    P: Fn(ValidPieceIndex) -> bool,
    S: Fn(ValidPieceIndex) -> bool,
    R: FnMut(ValidPieceIndex) -> bool,
{
    /// The peer requesting a piece.
    pub peer: PeerHandle,
    /// The peer's average piece download time (for steal calculations).
    pub peer_avg_time: Option<Duration>,
    /// Priority pieces to check first (e.g., for streaming).
    pub priority_pieces: I,
    /// File download priority ordering.
    pub file_priorities: &'a FilePriorities,
    /// File metadata for iterating pieces.
    pub file_infos: &'a FileInfos,
    /// Returns true if the peer has the given piece.
    pub peer_has_piece: P,
    /// Returns true if the piece can be stolen (e.g., not locked for writing).
    pub can_steal: S,
    /// Called last, right before a piece is reserved; returns false to skip it.
    pub can_reserve: R,
    /// Whether to reserve queued pieces once no priority piece can be reserved.
    pub queued_pieces: bool,
    /// In-flight pieces urgent enough for several peers to fetch at once.
    pub shared_pieces: &'a [ValidPieceIndex],
}

/// Coordinates piece download state.
///
/// Wraps a [`ChunkTracker`] with tracking of which pieces are currently being downloaded
/// (in-flight) and by which peer. This ensures that:
///
/// - A piece is only assigned to one peer at a time (unless stolen)
/// - Pieces are properly requeued when a peer dies
/// - State transitions maintain invariants
pub struct PieceTracker {
    chunks: ChunkTracker,
    inflight: HashMap<ValidPieceIndex, InflightPiece>,
    /// Pieces that failed their hash check while shared: fetched from one
    /// peer until verified, so a corrupt peer can be told apart.
    unshareable: HashSet<ValidPieceIndex>,
}

impl PieceTracker {
    // === CONSTRUCTION ===

    /// Create a new PieceTracker wrapping the given ChunkTracker.
    pub fn new(chunks: ChunkTracker) -> Self {
        Self {
            chunks,
            inflight: HashMap::new(),
            unshareable: HashSet::new(),
        }
    }

    /// Read-only access to the underlying ChunkTracker.
    pub fn chunks(&self) -> &ChunkTracker {
        &self.chunks
    }

    /// Consume the PieceTracker, requeuing any in-flight pieces.
    ///
    /// This is used when pausing a torrent - any pieces that were being downloaded
    /// need to be put back in the queue so they can be re-downloaded on resume.
    pub fn into_chunks(mut self) -> ChunkTracker {
        // Requeue all in-flight pieces so they'll be re-downloaded on resume
        for piece in self.inflight.into_keys() {
            self.chunks.mark_piece_broken_if_not_have(piece);
        }
        self.chunks
    }

    // === PIECE ACQUISITION ===

    /// Attempt to acquire a piece for the requesting peer.
    ///
    /// The acquisition strategy is:
    /// 1. Try to steal a piece from a peer that's 10x slower
    /// 2. Go through the shared pieces, most urgent first: reserve one not
    ///    yet in flight, or join one in flight that has chunks left to take
    /// 3. Try to reserve a piece from the queue (priority pieces first)
    /// 4. Try to steal a piece from a peer that's 3x slower
    ///
    /// If `Stolen` is returned, the caller MUST call `peers.on_steal()` to notify
    /// the old peer and update counters.
    pub fn acquire_piece<I, P, S, R>(
        &mut self,
        mut req: AcquireRequest<I, P, S, R>,
    ) -> AcquireResult
    where
        I: Iterator<Item = ValidPieceIndex>,
        P: Fn(ValidPieceIndex) -> bool,
        S: Fn(ValidPieceIndex) -> bool,
        R: FnMut(ValidPieceIndex) -> bool,
    {
        // 1. Try steal with 10x threshold (very slow peer)
        if let Some(result) = self.try_steal(&req, 10.0) {
            return result;
        }

        // 2. The shared pieces come first, joined rather than passed over, so
        // a free peer helps the most urgent piece before starting a later one.
        if let Some(result) = self.try_shared(&mut req) {
            return result;
        }

        // 3. Try reserve from priority_pieces then queued pieces
        // First check priority pieces that aren't already downloaded or in-flight
        for piece in &mut req.priority_pieces {
            if !self.chunks.is_piece_have(piece)
                && !self.inflight.contains_key(&piece)
                && (req.peer_has_piece)(piece)
                && (req.can_reserve)(piece)
            {
                return self.reserve_piece(piece, req.peer);
            }
        }

        // Then check naturally ordered queued pieces
        // Note: iter_queued_pieces only returns pieces in queue_pieces (not in-flight)
        let queued: Vec<_> = if req.queued_pieces {
            self.chunks
                .iter_queued_pieces(req.file_priorities, req.file_infos)
                .collect()
        } else {
            Vec::new()
        };

        for piece in queued {
            if (req.peer_has_piece)(piece) && (req.can_reserve)(piece) {
                return self.reserve_piece(piece, req.peer);
            }
        }

        // 4. Try steal with 3x threshold (moderately slow peer)
        if let Some(result) = self.try_steal(&req, 3.0) {
            return result;
        }

        AcquireResult::NoneAvailable
    }

    /// Reserve a piece: remove from queue, add to inflight.
    fn reserve_piece(&mut self, piece: ValidPieceIndex, peer: PeerHandle) -> AcquireResult {
        self.chunks.reserve_needed_piece(piece);
        let chunks = self.chunks.get_lengths().chunks_per_piece(piece);
        self.inflight
            .insert(piece, InflightPiece::new(peer, chunks));
        AcquireResult::Reserved(piece)
    }

    /// Try to steal a piece from a slower peer.
    fn try_steal<I, P, S, R>(
        &mut self,
        req: &AcquireRequest<I, P, S, R>,
        threshold: f64,
    ) -> Option<AcquireResult>
    where
        I: Iterator<Item = ValidPieceIndex>,
        P: Fn(ValidPieceIndex) -> bool,
        S: Fn(ValidPieceIndex) -> bool,
        R: FnMut(ValidPieceIndex) -> bool,
    {
        let my_avg = req.peer_avg_time?;
        let min_elapsed = Duration::from_secs_f64(my_avg.as_secs_f64() * threshold);

        // Find the slowest piece from another peer that exceeds threshold
        // and that the stealing peer actually has (can download)
        let (piece, old_peer, _) = self
            .inflight
            .iter()
            .filter(|(_, info)| info.peer != req.peer)
            .filter(|(p, _)| (req.peer_has_piece)(**p))
            .map(|(p, info)| (*p, info.peer, info.started.elapsed()))
            .filter(|(_, _, elapsed)| *elapsed >= min_elapsed)
            .max_by_key(|(_, _, elapsed)| *elapsed)?;

        // Check can_steal (e.g., per_piece_lock)
        if !(req.can_steal)(piece) {
            return None;
        }

        // Update ownership (piece stays in inflight, just changes owner).
        // The old owner's requests are cancelled; chunks already received
        // stay, so the new owner fetches only the rest. A piece this slow may
        // wait on a joiner too, so every request is free to make again.
        let info = self.inflight.get_mut(&piece)?;
        info.release_all_claims();
        info.joiners.retain(|joiner| *joiner != req.peer);
        info.peer = req.peer;
        info.started = Instant::now();

        Some(AcquireResult::Stolen {
            piece,
            from_peer: old_peer,
        })
    }

    /// Reserves the most urgent shared piece not yet in flight, or joins one
    /// in flight that `req.peer` can help fetch. A participant comes back to
    /// a shared piece it has requests left to make in.
    fn try_shared<I, P, S, R>(
        &mut self,
        req: &mut AcquireRequest<I, P, S, R>,
    ) -> Option<AcquireResult>
    where
        I: Iterator<Item = ValidPieceIndex>,
        P: Fn(ValidPieceIndex) -> bool,
        S: Fn(ValidPieceIndex) -> bool,
        R: FnMut(ValidPieceIndex) -> bool,
    {
        for &piece in req.shared_pieces {
            if self.chunks.is_piece_have(piece) || !(req.peer_has_piece)(piece) {
                continue;
            }
            let Some(info) = self.inflight.get_mut(&piece) else {
                if (req.can_reserve)(piece) {
                    let reserved = self.reserve_piece(piece, req.peer);
                    if !self.unshareable.contains(&piece)
                        && let Some(info) = self.inflight.get_mut(&piece)
                    {
                        info.shared = true;
                    }
                    return Some(reserved);
                }
                continue;
            };
            if self.unshareable.contains(&piece) {
                continue;
            }
            let participant = info.is_participant(req.peer);
            if !participant && info.joiners.len() + 1 >= MAX_SHARED_PARTICIPANTS {
                continue;
            }
            if !matches!(
                info.next_claim(&self.chunks, piece, req.peer),
                ChunkClaim::Chunk { .. }
            ) {
                continue;
            }
            if !participant {
                info.joiners.push(req.peer);
                info.shared = true;
            }
            return Some(AcquireResult::Joined(piece));
        }
        None
    }

    /// Claims the next chunk of `piece` for `peer` to request, as
    /// [`InflightPiece::next_claim`] chooses it. [`ChunkClaim::Done`] once
    /// `peer` no longer takes part in the piece or has nothing left to
    /// request.
    pub fn claim_chunk(&mut self, piece: ValidPieceIndex, peer: PeerHandle) -> ChunkClaim {
        let Some(info) = self.inflight.get_mut(&piece) else {
            return ChunkClaim::Done;
        };
        if !info.is_participant(peer) {
            return ChunkClaim::Done;
        }
        let claim = info.next_claim(&self.chunks, piece, peer);
        if let ChunkClaim::Chunk { index, .. } = claim {
            info.claim(index, peer);
        }
        claim
    }

    /// The participants other than `peer` that requested `chunk` of `piece`,
    /// whose copies are no longer needed once `peer`'s arrived.
    pub fn other_claimants(
        &self,
        piece: ValidPieceIndex,
        chunk: usize,
        peer: PeerHandle,
    ) -> Vec<PeerHandle> {
        self.inflight
            .get(&piece)
            .and_then(|info| info.claims.get(chunk))
            .into_iter()
            .flatten()
            .flatten()
            .map(|claim| claim.peer)
            .filter(|claimant| *claimant != peer)
            .collect()
    }

    /// Removes a peer that choked us from the shared pieces it takes part
    /// in, since it will not answer their requests, so the other
    /// participants request its chunks. Returns those pieces, whose requests
    /// to `peer` can be cancelled. A piece `peer` fetches alone stays its
    /// own until stolen.
    pub fn leave_shared_pieces(&mut self, peer: PeerHandle) -> Vec<ValidPieceIndex> {
        self.inflight
            .iter_mut()
            .filter(|(_, info)| info.is_participant(peer))
            .filter_map(|(piece, info)| info.leave(peer).then_some(*piece))
            .collect()
    }

    // === PIECE COMPLETION ===

    /// Remove piece from inflight tracking (e.g., after all chunks received).
    ///
    /// Returns download duration if piece was in-flight.
    /// Note: Does NOT mark the piece as downloaded - caller should do hash check
    /// and then call `mark_piece_hash_ok` or `mark_piece_hash_failed`.
    pub fn take_inflight(&mut self, piece: ValidPieceIndex) -> Option<InflightPiece> {
        self.inflight.remove(&piece)
    }

    /// Mark piece as downloaded after successful hash verification.
    pub fn mark_piece_hash_ok(&mut self, piece: ValidPieceIndex) {
        self.unshareable.remove(&piece);
        self.chunks.mark_piece_downloaded(piece);
    }

    /// Mark a piece we have as missing again - requeues the piece if it's selected.
    /// Returns false if we didn't have it.
    pub fn mark_piece_missing(&mut self, piece: ValidPieceIndex, file_infos: &FileInfos) -> bool {
        self.chunks.mark_piece_missing(piece, file_infos)
    }

    /// Mark piece as failed after hash verification failure - requeues the piece.
    pub fn mark_piece_hash_failed(&mut self, piece: ValidPieceIndex) {
        self.chunks.mark_piece_broken_if_not_have(piece);
    }

    /// Mark a piece several peers fetched as failed: requeue it, and fetch it
    /// from one peer next time, since its corrupt chunks cannot be traced.
    pub fn mark_shared_piece_hash_failed(&mut self, piece: ValidPieceIndex) {
        self.unshareable.insert(piece);
        self.chunks.mark_piece_broken_if_not_have(piece);
    }

    /// Release all pieces owned by a peer (on peer death).
    ///
    /// Moves all pieces owned by the peer from IN_FLIGHT back to QUEUED.
    /// Returns the pieces released.
    pub fn release_pieces_owned_by(&mut self, peer: PeerHandle) -> Vec<ValidPieceIndex> {
        // A shared piece passes to a joiner, keeping its downloaded chunks,
        // and a joiner leaving frees only its own requests.
        self.leave_shared_pieces(peer);
        // Collect pieces to release (can't modify while iterating)
        let pieces_to_release: Vec<_> = self
            .inflight
            .iter()
            .filter(|(_, info)| info.peer == peer)
            .map(|(p, _)| *p)
            .collect();

        for &piece in &pieces_to_release {
            self.inflight.remove(&piece);
            self.chunks.mark_piece_broken_if_not_have(piece);
        }
        pieces_to_release
    }

    // === QUERIES ===

    /// Get the inflight info for a piece, if it's currently being downloaded.
    pub fn get_inflight(&self, piece: ValidPieceIndex) -> Option<&InflightPiece> {
        self.inflight.get(&piece)
    }

    /// The pieces currently in-flight.
    pub fn inflight_pieces(&self) -> impl Iterator<Item = ValidPieceIndex> + '_ {
        self.inflight.keys().copied()
    }

    /// Check if a piece is currently in-flight.
    #[allow(dead_code)]
    pub fn is_inflight(&self, piece: ValidPieceIndex) -> bool {
        self.inflight.contains_key(&piece)
    }

    /// Get the number of pieces currently in-flight.
    #[allow(dead_code)]
    pub fn inflight_count(&self) -> usize {
        self.inflight.len()
    }

    // === PASS-THROUGH METHODS ===

    /// Mark a chunk as downloaded. Returns the result indicating if the piece is complete.
    pub fn mark_chunk_downloaded(
        &mut self,
        piece: &Piece<ByteBuf<'_>>,
    ) -> Option<ChunkMarkingResult> {
        self.chunks.mark_chunk_downloaded(piece)
    }

    /// Update which files are selected for download.
    pub fn update_only_files(
        &mut self,
        file_infos: &FileInfos,
        new_only_files: &HashSet<usize>,
    ) -> anyhow::Result<crate::chunk_tracker::HaveNeededSelected> {
        self.chunks.update_only_files(file_infos, new_only_files)
    }

    /// Update per-file have bytes when a piece completes. Returns remaining bytes for the file.
    pub fn update_file_have_on_piece_completed(
        &mut self,
        piece_id: ValidPieceIndex,
        file_id: usize,
        file_info: &FileInfo,
    ) -> u64 {
        self.chunks
            .update_file_have_on_piece_completed(piece_id, file_id, file_info)
    }

    /// Flush the have pieces bitfield to disk.
    pub fn flush_have_pieces(&mut self, flush_async: bool) -> anyhow::Result<()> {
        self.chunks.get_have_pieces_mut().flush(flush_async)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{bitv::BitV as BitVTrait, type_aliases::BF};
    use librqbit_core::lengths::Lengths;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn peer(id: u8) -> PeerHandle {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, id)), 6881)
    }

    /// Create a simple ChunkTracker for testing.
    /// Creates a torrent with the specified number of pieces, all selected.
    fn make_test_chunk_tracker(num_pieces: u32) -> ChunkTracker {
        // Create a simple single-file torrent
        let piece_length = 16384u32; // 16KB pieces
        let total_length = piece_length as u64 * num_pieces as u64;

        let lengths = Lengths::new(total_length, piece_length).unwrap();

        let bf_len = lengths.piece_bitfield_bytes();

        // No pieces downloaded yet (empty have)
        let have = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());

        // All pieces selected
        let mut selected = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        for i in 0..num_pieces as usize {
            selected.set(i, true);
        }

        // Single file spanning all pieces
        let file_infos = vec![crate::file_info::FileInfo {
            relative_filename: "test.dat".into(),
            offset_in_torrent: 0,
            len: total_length,
            piece_range: 0..num_pieces,
            attrs: Default::default(),
        }];

        ChunkTracker::new(have.into_dyn(), selected, lengths, &file_infos).unwrap()
    }

    fn make_test_file_infos(num_pieces: u32) -> FileInfos {
        let piece_length = 16384u64;
        vec![crate::file_info::FileInfo {
            relative_filename: "test.dat".into(),
            offset_in_torrent: 0,
            len: piece_length * num_pieces as u64,
            piece_range: 0..num_pieces,
            attrs: Default::default(),
        }]
    }

    fn make_default_file_priorities(file_infos: &FileInfos) -> FilePriorities {
        (0..file_infos.len()).collect()
    }

    /// A tracker of `num_pieces` pieces of four chunks each, all selected.
    fn make_four_chunk_tracker(num_pieces: u32) -> (PieceTracker, FileInfos) {
        make_tracker(num_pieces, 4)
    }

    /// A tracker of `num_pieces` pieces of `chunks` chunks each, all selected.
    fn make_tracker(num_pieces: u32, chunks: u32) -> (PieceTracker, FileInfos) {
        let piece_length = chunks * 16384u32;
        let total_length = piece_length as u64 * num_pieces as u64;
        let lengths = Lengths::new(total_length, piece_length).unwrap();
        let bf_len = lengths.piece_bitfield_bytes();
        let have = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        let mut selected = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        for i in 0..num_pieces as usize {
            selected.set(i, true);
        }
        let file_infos = vec![crate::file_info::FileInfo {
            relative_filename: "test.dat".into(),
            offset_in_torrent: 0,
            len: total_length,
            piece_range: 0..num_pieces,
            attrs: Default::default(),
        }];
        let chunks = ChunkTracker::new(have.into_dyn(), selected, lengths, &file_infos).unwrap();
        (PieceTracker::new(chunks), file_infos)
    }

    /// Acquires for `who` with `priority` and `shared` pieces and no queued ones.
    fn acquire_with(
        tracker: &mut PieceTracker,
        file_infos: &FileInfos,
        who: PeerHandle,
        priority: &[ValidPieceIndex],
        shared: &[ValidPieceIndex],
    ) -> AcquireResult {
        let file_priorities = make_default_file_priorities(file_infos);
        tracker.acquire_piece(AcquireRequest {
            peer: who,
            peer_avg_time: None,
            priority_pieces: priority.iter().copied(),
            file_priorities: &file_priorities,
            file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
            can_reserve: |_| true,
            queued_pieces: false,
            shared_pieces: shared,
        })
    }

    fn first_piece(tracker: &PieceTracker) -> ValidPieceIndex {
        tracker
            .chunks()
            .get_lengths()
            .validate_piece_index(0)
            .unwrap()
    }

    /// Claims for each of `who` in turn.
    fn claims(
        tracker: &mut PieceTracker,
        piece: ValidPieceIndex,
        who: &[PeerHandle],
    ) -> Vec<Option<usize>> {
        who.iter()
            .map(|peer| match tracker.claim_chunk(piece, *peer) {
                ChunkClaim::Chunk { index, .. } => Some(index),
                ChunkClaim::Full | ChunkClaim::Done => None,
            })
            .collect()
    }

    fn chunk_arrives(tracker: &mut PieceTracker, piece: ValidPieceIndex, chunk: u32) {
        let block = [0u8; 16384];
        tracker.mark_chunk_downloaded(&Piece::from_data(piece.get(), chunk * 16384, &block));
    }

    #[test]
    fn a_shared_piece_is_fetched_from_both_ends() {
        let (mut tracker, file_infos) = make_four_chunk_tracker(2);
        let piece_0 = first_piece(&tracker);
        assert!(matches!(
            acquire_with(&mut tracker, &file_infos, peer(1), &[piece_0], &[]),
            AcquireResult::Reserved(p) if p == piece_0
        ));
        assert!(
            matches!(
                acquire_with(&mut tracker, &file_infos, peer(2), &[], &[]),
                AcquireResult::NoneAvailable
            ),
            "a piece not offered for sharing is not joined"
        );
        assert!(matches!(
            acquire_with(&mut tracker, &file_infos, peer(2), &[], &[piece_0]),
            AcquireResult::Joined(p) if p == piece_0
        ));
        assert_eq!(
            claims(&mut tracker, piece_0, &[peer(1), peer(2)].repeat(3)),
            [Some(0), Some(3), Some(1), Some(2), None, None],
            "the owner goes forwards and the joiner backwards, each chunk once, \
             and with two requests each neither duplicates"
        );
        assert_eq!(
            tracker.claim_chunk(piece_0, peer(3)),
            ChunkClaim::Done,
            "a peer taking no part claims nothing"
        );
    }

    #[test]
    fn a_peer_out_of_chunks_duplicates_the_latest_requests() {
        let (mut tracker, file_infos) = make_four_chunk_tracker(1);
        let piece_0 = first_piece(&tracker);
        acquire_with(&mut tracker, &file_infos, peer(1), &[piece_0], &[]);
        assert_eq!(
            claims(&mut tracker, piece_0, &[peer(1); 4]),
            [Some(0), Some(1), Some(2), Some(3)]
        );
        assert!(
            matches!(
                acquire_with(&mut tracker, &file_infos, peer(2), &[], &[piece_0]),
                AcquireResult::Joined(_)
            ),
            "a piece whose chunks the owner requested can still be joined"
        );
        assert_eq!(
            claims(&mut tracker, piece_0, &[peer(2); 3]),
            [Some(3), Some(2), None],
            "the joiner duplicates the owner's last two requests"
        );
        assert_eq!(
            tracker.claim_chunk(piece_0, peer(1)),
            ChunkClaim::Done,
            "the owner holds more than two requests"
        );

        chunk_arrives(&mut tracker, piece_0, 3);
        assert_eq!(
            tracker.other_claimants(piece_0, 3, peer(2)),
            [peer(1)],
            "the owner's copy can be cancelled"
        );
        assert_eq!(
            tracker.claim_chunk(piece_0, peer(2)),
            ChunkClaim::Chunk {
                index: 1,
                duplicate: true
            },
            "with one request left, the joiner duplicates another"
        );
        assert!(
            matches!(
                acquire_with(&mut tracker, &file_infos, peer(2), &[], &[piece_0]),
                AcquireResult::NoneAvailable
            ),
            "a participant holding two requests looks elsewhere"
        );
    }

    #[test]
    fn a_chunk_is_requested_from_at_most_two_peers() {
        let (mut tracker, file_infos) = make_four_chunk_tracker(1);
        let piece_0 = first_piece(&tracker);
        acquire_with(&mut tracker, &file_infos, peer(1), &[piece_0], &[]);
        claims(&mut tracker, piece_0, &[peer(1); 4]);
        for joiner in [2, 3] {
            acquire_with(&mut tracker, &file_infos, peer(joiner), &[], &[piece_0]);
        }
        assert_eq!(
            claims(&mut tracker, piece_0, &[peer(2), peer(3), peer(2), peer(3)]),
            [Some(3), Some(2), Some(1), Some(0)],
            "each joiner duplicates chunks the other has not"
        );
        assert_eq!(
            claims(&mut tracker, piece_0, &[peer(2), peer(3)]),
            [None, None]
        );
    }

    #[test]
    fn a_shared_piece_leaves_most_of_its_chunks_to_the_peers_joining_it() {
        let (mut tracker, file_infos) = make_tracker(2, 64);
        let piece_0 = first_piece(&tracker);
        let piece_1 = tracker
            .chunks()
            .get_lengths()
            .validate_piece_index(1)
            .unwrap();
        assert!(matches!(
            acquire_with(&mut tracker, &file_infos, peer(1), &[], &[piece_0]),
            AcquireResult::Reserved(_)
        ));
        let owner = claims(&mut tracker, piece_0, &[peer(1); 16]);
        assert_eq!(owner, (0..16).map(Some).collect::<Vec<_>>());
        assert_eq!(
            tracker.claim_chunk(piece_0, peer(1)),
            ChunkClaim::Full,
            "the owner waits with sixteen requests"
        );
        acquire_with(&mut tracker, &file_infos, peer(2), &[], &[piece_0]);
        let joiner = claims(&mut tracker, piece_0, &[peer(2); 16]);
        assert_eq!(joiner, (48..64).rev().map(Some).collect::<Vec<_>>());
        assert_eq!(tracker.claim_chunk(piece_0, peer(2)), ChunkClaim::Full);

        chunk_arrives(&mut tracker, piece_0, 0);
        assert_eq!(
            tracker.claim_chunk(piece_0, peer(1)),
            ChunkClaim::Chunk {
                index: 16,
                duplicate: false
            },
            "an answered request makes room for the next"
        );

        acquire_with(&mut tracker, &file_infos, peer(3), &[piece_1], &[]);
        assert_eq!(
            claims(&mut tracker, piece_1, &[peer(3); 64]),
            (0..64).map(Some).collect::<Vec<_>>(),
            "a piece not shared is requested whole"
        );
    }

    #[test]
    fn a_shared_piece_takes_at_most_three_peers() {
        let (mut tracker, file_infos) = make_four_chunk_tracker(1);
        let piece_0 = first_piece(&tracker);
        acquire_with(&mut tracker, &file_infos, peer(1), &[piece_0], &[]);
        for joiner in [2, 3] {
            assert!(matches!(
                acquire_with(&mut tracker, &file_infos, peer(joiner), &[], &[piece_0]),
                AcquireResult::Joined(_)
            ));
        }
        assert!(matches!(
            acquire_with(&mut tracker, &file_infos, peer(4), &[], &[piece_0]),
            AcquireResult::NoneAvailable
        ));
    }

    #[test]
    fn a_choking_peer_leaves_its_shared_pieces() {
        let (mut tracker, file_infos) = make_four_chunk_tracker(2);
        let piece_0 = first_piece(&tracker);
        let piece_1 = tracker
            .chunks()
            .get_lengths()
            .validate_piece_index(1)
            .unwrap();
        acquire_with(&mut tracker, &file_infos, peer(1), &[piece_0], &[]);
        acquire_with(&mut tracker, &file_infos, peer(2), &[], &[piece_0]);
        acquire_with(&mut tracker, &file_infos, peer(3), &[piece_1], &[]);
        claims(&mut tracker, piece_0, &[peer(1), peer(2), peer(2)]);

        assert_eq!(tracker.leave_shared_pieces(peer(2)), [piece_0]);
        assert_eq!(
            claims(&mut tracker, piece_0, &[peer(2), peer(1), peer(1)]),
            [None, Some(1), Some(2)],
            "the joiner's requests are the owner's to make"
        );
        assert!(
            tracker.leave_shared_pieces(peer(3)).is_empty(),
            "a piece fetched alone waits for its peer or a steal"
        );
        assert_eq!(tracker.get_inflight(piece_1).unwrap().peer, peer(3));

        acquire_with(&mut tracker, &file_infos, peer(2), &[], &[piece_0]);
        assert_eq!(tracker.leave_shared_pieces(peer(1)), [piece_0]);
        assert_eq!(
            tracker.get_inflight(piece_0).unwrap().peer,
            peer(2),
            "an owner that choked hands the piece over"
        );
        assert_eq!(
            claims(&mut tracker, piece_0, &[peer(2); 4]),
            [Some(0), Some(1), Some(2), Some(3)]
        );
    }

    #[test]
    fn a_steal_frees_every_participants_requests() {
        let (mut tracker, file_infos) = make_four_chunk_tracker(1);
        let piece_0 = first_piece(&tracker);
        acquire_with(&mut tracker, &file_infos, peer(1), &[piece_0], &[]);
        acquire_with(&mut tracker, &file_infos, peer(2), &[], &[piece_0]);
        claims(&mut tracker, piece_0, &[peer(1), peer(2), peer(1), peer(2)]);

        let file_priorities = make_default_file_priorities(&file_infos);
        let stolen = tracker.acquire_piece(AcquireRequest {
            peer: peer(3),
            peer_avg_time: Some(Duration::ZERO),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
            can_reserve: |_| true,
            queued_pieces: false,
            shared_pieces: &[],
        });
        assert!(matches!(
            stolen,
            AcquireResult::Stolen { piece, from_peer } if piece == piece_0 && from_peer == peer(1)
        ));
        assert_eq!(
            claims(&mut tracker, piece_0, &[peer(3); 4]),
            [Some(0), Some(1), Some(2), Some(3)],
            "a joiner's requests do not hold back the new owner"
        );
    }

    #[test]
    fn a_shared_piece_passes_to_a_joiner_when_its_owner_dies() {
        let (mut tracker, file_infos) = make_four_chunk_tracker(1);
        let piece_0 = first_piece(&tracker);
        acquire_with(&mut tracker, &file_infos, peer(1), &[piece_0], &[]);
        acquire_with(&mut tracker, &file_infos, peer(2), &[], &[piece_0]);
        assert_eq!(
            claims(&mut tracker, piece_0, &[peer(1), peer(2)]),
            [Some(0), Some(3)]
        );

        assert!(
            tracker.release_pieces_owned_by(peer(1)).is_empty(),
            "the shared piece is not requeued"
        );
        let info = tracker.get_inflight(piece_0).unwrap();
        assert_eq!((info.peer, info.joiners.len()), (peer(2), 0));
        assert_eq!(
            claims(&mut tracker, piece_0, &[peer(2); 3]),
            [Some(0), Some(1), Some(2)],
            "the dead owner's request is free again, and the new owner keeps its own"
        );
    }

    #[test]
    fn a_shared_piece_that_failed_its_check_is_fetched_from_one_peer() {
        let (mut tracker, file_infos) = make_four_chunk_tracker(1);
        let piece_0 = first_piece(&tracker);
        acquire_with(&mut tracker, &file_infos, peer(1), &[piece_0], &[]);
        acquire_with(&mut tracker, &file_infos, peer(2), &[], &[piece_0]);
        tracker.take_inflight(piece_0);
        tracker.mark_shared_piece_hash_failed(piece_0);

        assert!(matches!(
            acquire_with(&mut tracker, &file_infos, peer(1), &[piece_0], &[]),
            AcquireResult::Reserved(_)
        ));
        assert!(matches!(
            acquire_with(&mut tracker, &file_infos, peer(2), &[], &[piece_0]),
            AcquireResult::NoneAvailable
        ));
    }

    #[test]
    fn test_new_piece_tracker() {
        let chunks = make_test_chunk_tracker(10);
        let tracker = PieceTracker::new(chunks);
        assert_eq!(tracker.inflight_count(), 0);
    }

    #[test]
    fn test_a_piece_the_storage_cannot_reserve_is_skipped() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);
        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);
        let lengths = *tracker.chunks().get_lengths();
        let piece = |i| lengths.validate_piece_index(i).unwrap();
        let mut asked = Vec::new();

        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            peer_avg_time: None,
            priority_pieces: [piece(3), piece(4)].into_iter(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
            can_reserve: |p| {
                asked.push(p.get());
                p.get() != 3
            },
            queued_pieces: true,
            shared_pieces: &[],
        });
        assert!(matches!(result, AcquireResult::Reserved(p) if p.get() == 4));
        assert_eq!(asked, [3, 4]);
        assert!(
            !tracker.is_inflight(piece(3)),
            "a refused piece stays queued"
        );

        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
            can_reserve: |_| true,
            queued_pieces: false,
            shared_pieces: &[],
        });
        assert!(
            matches!(result, AcquireResult::NoneAvailable),
            "only priority pieces are requested without queued pieces"
        );
    }

    #[test]
    fn test_reserve_piece_from_queue() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true, // Peer has all pieces
            can_steal: |_| true,
            can_reserve: |_| true,
            queued_pieces: true,
            shared_pieces: &[],
        });

        // Should reserve piece 0 (first in queue)
        match result {
            AcquireResult::Reserved(piece) => {
                assert_eq!(piece.get(), 0);
                assert!(tracker.is_inflight(piece));
                assert_eq!(tracker.inflight_count(), 1);
            }
            _ => panic!("Expected Reserved, got {:?}", result),
        }
    }

    #[test]
    fn test_reserve_filters_by_peer_has_piece() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Peer only has piece 2 and later
        // Note: iter_queued_pieces iterates in order: first, last, middle
        // So for 0..5: 0, 4, 1, 2, 3
        // With filter >= 2, we get 4 first (skips 0, takes 4)
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |p| p.get() >= 2,
            can_steal: |_| true,
            can_reserve: |_| true,
            queued_pieces: true,
            shared_pieces: &[],
        });

        match result {
            AcquireResult::Reserved(piece) => {
                // Got piece 4 (first one peer has in iteration order)
                assert!(piece.get() >= 2, "Should have gotten a piece >= 2");
            }
            _ => panic!("Expected Reserved, got {:?}", result),
        }
    }

    #[test]
    fn test_complete_piece() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Reserve a piece first
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
            can_reserve: |_| true,
            queued_pieces: true,
            shared_pieces: &[],
        });

        let piece = match result {
            AcquireResult::Reserved(p) => p,
            _ => panic!("Expected Reserved"),
        };

        // Complete the piece (take_inflight + hash check + mark_piece_hash_ok)
        let duration = tracker.take_inflight(piece);
        assert!(duration.is_some());
        assert!(!tracker.is_inflight(piece));
        // Simulate successful hash check
        tracker.mark_piece_hash_ok(piece);
        assert!(tracker.chunks().is_piece_have(piece));
    }

    #[test]
    fn test_fail_piece_requeues() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Reserve piece 0
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
            can_reserve: |_| true,
            queued_pieces: true,
            shared_pieces: &[],
        });

        let piece = match result {
            AcquireResult::Reserved(p) => p,
            _ => panic!("Expected Reserved"),
        };

        assert!(tracker.is_inflight(piece));

        // Fail the piece (take_inflight + hash check fails + mark_piece_hash_failed)
        let duration = tracker.take_inflight(piece);
        assert!(duration.is_some());
        // Simulate failed hash check
        tracker.mark_piece_hash_failed(piece);

        // Should no longer be in-flight
        assert!(!tracker.is_inflight(piece));
        // Should not be in have
        assert!(!tracker.chunks().is_piece_have(piece));
        // Should be back in queue - verify by trying to reserve it again
        let result2 = tracker.acquire_piece(AcquireRequest {
            peer: peer(2),
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |p| p == piece, // Only has the failed piece
            can_steal: |_| true,
            can_reserve: |_| true,
            queued_pieces: true,
            shared_pieces: &[],
        });

        match result2 {
            AcquireResult::Reserved(p) => assert_eq!(p, piece),
            _ => panic!("Expected piece to be re-reservable after fail"),
        }
    }

    #[test]
    fn test_release_pieces_owned_by_peer() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        let peer_a = peer(1);
        let peer_b = peer(2);

        // Peer A reserves first two pieces (order: 0, 4, 1, 2, 3)
        // So peer A gets pieces 0 and 4
        let piece_a1 = match tracker.acquire_piece(AcquireRequest {
            peer: peer_a,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
            can_reserve: |_| true,
            queued_pieces: true,
            shared_pieces: &[],
        }) {
            AcquireResult::Reserved(p) => p,
            _ => panic!("Expected Reserved"),
        };
        let piece_a2 = match tracker.acquire_piece(AcquireRequest {
            peer: peer_a,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
            can_reserve: |_| true,
            queued_pieces: true,
            shared_pieces: &[],
        }) {
            AcquireResult::Reserved(p) => p,
            _ => panic!("Expected Reserved"),
        };

        // Peer B reserves next piece
        let piece_b = match tracker.acquire_piece(AcquireRequest {
            peer: peer_b,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
            can_reserve: |_| true,
            queued_pieces: true,
            shared_pieces: &[],
        }) {
            AcquireResult::Reserved(p) => p,
            _ => panic!("Expected Reserved"),
        };

        assert_eq!(tracker.inflight_count(), 3);
        assert!(tracker.is_inflight(piece_a1));
        assert!(tracker.is_inflight(piece_a2));
        assert!(tracker.is_inflight(piece_b));

        // Peer A dies
        let released = tracker.release_pieces_owned_by(peer_a);
        assert_eq!(released.len(), 2);
        assert_eq!(tracker.inflight_count(), 1); // Only peer B's piece remains

        // Verify peer B's piece is still in-flight
        assert!(tracker.is_inflight(piece_b));
        // Verify peer A's pieces are no longer in-flight
        assert!(!tracker.is_inflight(piece_a1));
        assert!(!tracker.is_inflight(piece_a2));
    }

    #[test]
    fn test_into_chunks_requeues_inflight() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Reserve pieces 0 and 1
        tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
            can_reserve: |_| true,
            queued_pieces: true,
            shared_pieces: &[],
        });
        tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
            can_reserve: |_| true,
            queued_pieces: true,
            shared_pieces: &[],
        });

        assert_eq!(tracker.inflight_count(), 2);

        // Convert back to chunks (simulates pause)
        let chunks = tracker.into_chunks();

        // Create a new tracker and verify pieces are back in queue
        let mut new_tracker = PieceTracker::new(chunks);
        let result = new_tracker.acquire_piece(AcquireRequest {
            peer: peer(2),
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
            can_reserve: |_| true,
            queued_pieces: true,
            shared_pieces: &[],
        });

        // Should get piece 0 again (was requeued)
        match result {
            AcquireResult::Reserved(p) => assert_eq!(p.get(), 0),
            _ => panic!("Expected to reserve piece 0 after into_chunks"),
        }
    }

    #[test]
    fn test_priority_pieces_checked_first() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Priority pieces: piece 3, then piece 2
        let piece3 = tracker
            .chunks()
            .get_lengths()
            .validate_piece_index(3)
            .unwrap();
        let piece2 = tracker
            .chunks()
            .get_lengths()
            .validate_piece_index(2)
            .unwrap();
        let priority = vec![piece3, piece2];

        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            peer_avg_time: None,
            priority_pieces: priority.into_iter(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
            can_reserve: |_| true,
            queued_pieces: true,
            shared_pieces: &[],
        });

        // Should get piece 3 (first priority piece)
        match result {
            AcquireResult::Reserved(p) => assert_eq!(p.get(), 3),
            _ => panic!("Expected Reserved(3), got {:?}", result),
        }
    }

    #[test]
    fn test_none_available_when_no_pieces() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        // Peer has no pieces
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| false, // Peer has nothing
            can_steal: |_| true,
            can_reserve: |_| true,
            queued_pieces: true,
            shared_pieces: &[],
        });

        match result {
            AcquireResult::NoneAvailable => {}
            _ => panic!("Expected NoneAvailable, got {:?}", result),
        }
    }

    #[test]
    fn test_take_inflight_nonexistent_piece_returns_none() {
        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);
        let piece = tracker
            .chunks()
            .get_lengths()
            .validate_piece_index(0)
            .unwrap();

        // Try to take a piece that's not in-flight
        let result = tracker.take_inflight(piece);
        assert!(result.is_none());
    }

    #[test]
    fn test_steal_only_pieces_peer_has() {
        // This test verifies the fix for a bug where try_steal didn't check
        // if the stealing peer actually has the piece in their bitfield.
        // Without this check, a peer could "steal" a piece they can't download,
        // leaving it stuck in inflight forever.

        let chunks = make_test_chunk_tracker(5);
        let mut tracker = PieceTracker::new(chunks);

        let file_infos = make_test_file_infos(5);
        let file_priorities = make_default_file_priorities(&file_infos);

        let peer_a = peer(1);
        let peer_b = peer(2);

        // Peer A reserves pieces 0 and 4 (first two in iteration order)
        let piece_0 = match tracker.acquire_piece(AcquireRequest {
            peer: peer_a,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
            can_reserve: |_| true,
            queued_pieces: true,
            shared_pieces: &[],
        }) {
            AcquireResult::Reserved(p) => {
                assert_eq!(p.get(), 0);
                p
            }
            _ => panic!("Expected Reserved"),
        };

        let piece_4 = match tracker.acquire_piece(AcquireRequest {
            peer: peer_a,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
            can_reserve: |_| true,
            queued_pieces: true,
            shared_pieces: &[],
        }) {
            AcquireResult::Reserved(p) => {
                assert_eq!(p.get(), 4);
                p
            }
            _ => panic!("Expected Reserved"),
        };

        // Sleep briefly so pieces become stealable
        std::thread::sleep(Duration::from_millis(5));

        // Peer B tries to acquire with:
        // - Very short avg_time (1ms) so 3x threshold = 3ms < 5ms elapsed
        // - peer_has_piece returns true ONLY for piece 4, NOT piece 0
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer_b,
            peer_avg_time: Some(Duration::from_millis(1)),
            priority_pieces: std::iter::empty(),
            file_priorities: &file_priorities,
            file_infos: &file_infos,
            peer_has_piece: |p| p.get() == 4, // Peer B only has piece 4
            can_steal: |_| true,
            can_reserve: |_| true,
            queued_pieces: true,
            shared_pieces: &[],
        });

        // Should steal piece 4 (which peer B has), NOT piece 0 (which peer B doesn't have)
        match result {
            AcquireResult::Stolen { piece, from_peer } => {
                assert_eq!(piece, piece_4, "Should steal piece 4 (the one peer B has)");
                assert_eq!(from_peer, peer_a);
                // Verify piece 0 is still owned by peer A (wasn't stolen)
                assert_eq!(tracker.get_inflight(piece_0).unwrap().peer, peer_a);
            }
            _ => panic!("Expected Stolen, got {:?}", result),
        }
    }
}
