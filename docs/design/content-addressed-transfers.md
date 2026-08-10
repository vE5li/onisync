# Content-addressed, multi-source file transfers

## Status

Proposed. Supersedes the two current byte-movement paths (point-to-point
"pull" and the recursive "fetch" relay) with a single mechanism.

## Motivation

Live sync currently breaks in the topology `computer <-> central <-> phone`.
When the phone edits a file:

1. Central records the new version and **forwards the metadata announcement to
   the computer before it has pulled the bytes** (`lib.rs`
   `FileMetadataChanged`: `forward_to_peers` precedes `request_pull_from_origin`).
2. The computer immediately opens a point-to-point pull (`TransferStart`)
   against central for the new `content_hash`.
3. Central's on-disk copy is still the *previous* version, so its serve gate
   (`local_hash == content_hash`) fails and it aborts with
   `"file not available"`.
4. Central finishes its own pull ~1s later. The computer's pull is never
   retried, so it stays stale until the next full manifest reconcile.

Two root problems:

- **Point-to-point pulls cannot hop.** They target one named peer and abort if
  that peer lacks the bytes, even though another reachable peer (the phone) has
  them. There is a separate recursive fetch engine (`fetch.rs`) that *can* hop,
  but it is wired only to `tagsy edit`, not to live sync.
- **Two mechanisms.** Point-to-point pull and recursive fetch are separate code
  paths that both ultimately drive the *same* transfer primitive.

## Key insight

A chunk request that carries the **whole-file** content hash is
self-validating across peers: because the whole-file BLAKE3 hash pins the
entire byte sequence, the bytes of the canonical chunk at a given `offset` are
identical on every peer whose copy hashes to that value. Therefore:

- No per-chunk hashes are needed. The existing end-of-transfer full-file hash
  check remains the integrity backstop.
- Chunk *i* from peer A and chunk *j* from peer B are interchangeable **iff both
  A and B serve the same `content_hash`**.
- A serving peer needs only to verify *its own* copy matches `content_hash`
  before answering a chunk (it already does this on the serve side today).

This makes the **holder stateless per request** and lets the **receiver pull
each chunk from any holder** that currently has the matching content.

## Design

### One mechanism: content-addressed chunk multicast

There is one byte-movement mechanism. A chunk is treated as **pure content, not
a message**: the identity of a chunk *is* `(file_id, content_hash, offset)`, and
because chunking is deterministic (see below), that key denotes one exact,
bit-identical byte range. Nothing else correlates a request to its reply — no
transfer session, no per-request cookie, no open/close handshake.

Consequences of "a chunk is its content identity":

- **No correlation id on the wire.** A `ChunkData` reply is matched to a pending
  request purely by `(file_id, content_hash, offset)`.
- **Races are free.** If two replies for the same key arrive (e.g. an initial
  request flooded to two holders that both answer), they are bit-identical; take
  the first, drop the rest. Ordering and duplication never affect correctness.
- **Relays coalesce and multicast.** A relay keeps a *content-keyed waiter set*:
  "these downstream links are waiting for `(file_id, content_hash, offset)`."
  When a matching `ChunkData` arrives from any upstream, it is forwarded to
  *all* current waiters and the set is cleared. Two devices pulling the same
  file through one relay cause **one** upstream fetch, fanned out to both.

Discovery folds into the same protocol: a `ChunkRequest` *is* the probe. The
first chunk a receiver asks for floods across neighbours; whichever direction
returns `ChunkData` establishes the route for subsequent chunks. There is no
separate "who has it?" message — even the restore *availability probe* (which
wants existence without keeping bytes) is just an offset-0 `ChunkRequest` whose
result is discarded (see below).

The current point-to-point pull and the recursive fetch relay both collapse into
this: "point-to-point" is just a `ChunkRequest` to a neighbour that happens to
hold the file; "relayed fetch" is the same request forwarded hop-by-hop.

### Wire protocol (`tagsy-core`)

Bump the peer protocol version. Remove the transfer-id-scoped messages and add
content-addressed ones.

Remove:

```
Sync::TransferStart        { transfer_id, file_id, content_hash }
Sync::TransferChunkRequest { transfer_id, offset }
Sync::TransferChunk        { transfer_id, offset, bytes, last }
Sync::TransferAbort        { transfer_id, reason }
Sync::FetchRequest         { request_id, file_id, expected_hash }
Sync::FetchFound           { request_id, file_id, content_hash, size }
Sync::FetchMissing         { request_id }
```

Add content-addressed chunk transfer:

```
Sync::ChunkRequest { file_id, content_hash, offset }
Sync::ChunkData    { file_id, content_hash, offset, bytes }
Sync::ChunkMiss    { file_id, content_hash, offset }
```

- **No `request_id`.** The tuple `(file_id, content_hash, offset)` is the
  request's identity and the reply's routing key.
- **No `len` on the request and no `last` on the reply.** Both are derivable and
  therefore off the wire. `content_hash` **and** `expected_size` are equally
  authoritative catalog facts (they travel together on every version and are
  mutually consistent — the hash pins the exact byte sequence, hence the exact
  size). Given them:
  - Chunking is fixed and deterministic: `offset` MUST be a multiple of
    `CHUNK_SIZE`; the canonical chunk length is `min(CHUNK_SIZE,
    expected_size - offset)`, computed identically by every holder and every
    waiter. This is what makes `(file_id, content_hash, offset)` denote one
    canonical, bit-identical byte range so a single `ChunkData` fans out to all
    waiters for that key.
  - A holder derives the length from its own verified file size (which equals
    `expected_size`) and serves exactly that many bytes, or `ChunkMiss`.
  - The receiver knows the file boundary up front (`expected_size`) and
    terminates when it has written `expected_size` bytes. There is no per-chunk
    EOF flag. A zero-length file is one request at `offset = 0` returning an
    empty `ChunkData`.
- A responder answers `ChunkData` only if its local copy verifies against
  `content_hash` (see Serve side); otherwise `ChunkMiss`.
- `ChunkMiss` means "this direction cannot (any longer) serve this key" — either
  the node lacks the content and every upstream it forwarded to also missed, or
  it once had it but the file changed. It carries no reason string; the receiver
  reacts to it structurally (a chunk missing from *all* directions fails the
  receive), not by inspecting text.

### Routing (content-keyed multicast, stateless cut-through)

Chosen model: **per-chunk relay through intermediate nodes**, cut-through, with
a content-keyed waiter table. Required by the topology (the phone reaches only
central). This is the same flood/relay graph `fetch.rs` uses today, re-keyed
from `request_id` to content and made fan-out.

A relay maintains, per outstanding `(file_id, content_hash, offset)`:

```
WaiterEntry {
    downstream: Set<Link>,        // links waiting for this key
    upstream_outstanding: Set<Link>,  // neighbours we forwarded to, awaiting reply
    deadline: Instant,            // TTL; see below
}
```

Behavior:

- **On `ChunkRequest` from a downstream link:**
  - If we hold and verify the content, answer `ChunkData` directly.
  - Else, if an entry for this key already exists, just add the link to
    `downstream` (request coalescing — no new upstream fetch).
  - Else, create the entry, add the link to `downstream`, and forward the
    `ChunkRequest` to all neighbours except the sender, recording them in
    `upstream_outstanding`. Arm the TTL.
- **On `ChunkData` from an upstream** for a key we have an entry for: forward it
  to *every* link in `downstream`, then drop the entry. (First-writer-wins;
  later duplicates find no entry and are dropped.)
- **On `ChunkMiss` from an upstream:** remove it from `upstream_outstanding`.
  When that set empties (all upstreams missed), fan `ChunkMiss` to all
  `downstream` links and drop the entry.
- **On link drop:** prune the link from every entry's `downstream` and
  `upstream_outstanding`; apply the same emptying rules.
- **On TTL expiry:** fan `ChunkMiss` to `downstream` and drop the entry.

**No temp file, no whole-file buffer on relays** — only the waiter table, whose
size is bounded by the number of distinct in-flight chunk keys (≈
`WINDOW * active_transfers`).

The exhaustion accounting (`upstream_outstanding` draining to empty) and the
link-drop pruning are the same responsibilities `fetch.rs` already implements
for `children_outstanding`; this design **relocates** them from a per-
`request_id` table to a per-content-key table rather than introducing new
machinery.

Because a relay holds none of the bytes, it verifies nothing: integrity is
**end-to-end**, checked once by the origin receiver against the full-file hash.

**Trust caveat.** A buggy or malicious upstream could return wrong bytes for a
chunk; a relay cannot detect this (it does not hold `content_hash`'s file) and
would fan the bad bytes out. Only the origin receiver catches it, at final
full-file verification, and must then re-pull. Tagsy assumes all peers are
trusted (same user, mutually authenticated at handshake), so this is acceptable;
there is deliberately no per-chunk attribution or blame.

### Serve side (holder)

A holder answers `ChunkRequest` by:

1. Checking a **verified-hash cache**: `path -> (mtime, size, hash)`. If the
   file backing `file_id` still has the cached `(mtime, size)` and the cached
   `hash == content_hash`, serve directly.
2. On cache miss, stream-hash the file (existing `FileBytes::hash`) and populate
   the cache. If it matches, serve; else `ChunkMiss`.
3. Serve `min(CHUNK_SIZE, size - offset)` bytes via
   `FileBytes::read_chunk_at(offset, len)` (existing; seek + bounded read,
   O(window) memory), where `size` is the holder's own verified file size. The
   responder validates that `offset` is `CHUNK_SIZE`-aligned and in range; a
   malformed or out-of-range request is answered with `ChunkMiss`. A holder
   whose file verifies to `content_hash` but whose size differs from the
   requester's `expected_size` is impossible for a consistent catalog version;
   any resulting shortfall is caught by the receiver's final full-file hash
   check, so no special handling is needed.

This preserves today's correctness gate (`local_hash == content_hash`) but makes
it per-request and cheap after the first verification. The cache is invalidated
by mtime/size change, so a file edited mid-serve stops matching and the holder
answers `ChunkMiss` (the chunk is then served by another holder, or the receive
fails if none has it). Providers (CLI upload)
and the (now-removed) fetch cache collapse into "things that can answer a
`ChunkRequest`": a provider registers a `ChunkSource` keyed by
`(file_id, content_hash)`; no separate `TransferStart` provider path.

### Receiver (the one driver)

Generalize `transfer.rs::receive_inner`; it stays the single driver but sources
each chunk by content instead of from a bound session:

- Inputs: `file_id`, `content_hash`, `expected_size`, a `temp_path`, and a way
  to send `ChunkRequest`s and receive `ChunkData`/`ChunkMiss`.
- Keeps the existing state: `write_offset`, `pending` reorder `BTreeMap`,
  in-flight window (`WINDOW`), incremental BLAKE3, final verify. The
  `may_request` EOF cap now uses `expected_size` as an authoritative bound (not
  a hint): the receiver requests offsets up to `expected_size` and completes
  when `write_offset == expected_size`. The old "size is a hint, `last` is
  authoritative" logic is removed.
- Requests are keyed by `(offset)` within this receive (the `file_id` /
  `content_hash` are fixed for the whole receive). A reply is matched by
  `(file_id, content_hash, offset)`; a duplicate `ChunkData` for an offset
  already written is dropped.
- Where each chunk's *first* request goes is a **routing policy**, not part of
  the driver: v1 sends every chunk toward the same chosen neighbour (the one that
  answered the first chunk / the announcing origin); if that direction is unknown
  it floods to all neighbours. Because the wire request is self-describing, a
  later v2 could spread the window across multiple neighbours with no protocol
  change.
- **No retries, no re-flood on miss** (see Resolved parameters). A `ChunkMiss`
  from all directions, or a link drop, or the per-chunk liveness timeout
  (`HOP_TIMEOUT`, reset on each successful write) fails the whole receive
  immediately. Recovery is external (a newer version announcement, or reconnect →
  reconcile).

Multi-source still works without retries: each chunk's first request already goes
to whoever currently holds it. This is what answers "serve chunk 2 from central
once it has it": while central lacks the bytes it answers `ChunkMiss` (and the
chunk is served from another holder / via relay); once central's own copy
completes and verifies, its `ChunkData` for *subsequent* offsets is accepted like
any other holder's — no session to renegotiate, no re-flood required.

### Transfer lifecycle for live sync

Replace `request_pull_from_origin` (point-to-point `StartReceive`) in the
`FileMetadataAdded` / `FileMetadataChanged` / `FileRestored` handlers with a
single call that:

1. Opens a content-addressed receive for `(file_id, content_hash)` into a temp
   file, directing chunk requests toward the announcing origin (the most likely
   holder); a chunk whose direction is unknown floods.
2. On completion, materializes via the existing `DaemonMessage::Materialize`
   placement (unchanged).
3. On failure (total `ChunkMiss`, link drop, or liveness timeout — see Resolved
   parameters), gives up for this announcement (no retry, per decision). The next
   manifest reconcile re-attempts on reconnect, as today.

Manifest reconcile (`reconcile_peer_manifest` -> `WantedFile`) uses the same
receive entry point.

`tagsy edit`'s fetch uses the same entry point; its only difference is the
terminal action: return the completed temp file to the CLI instead of
materializing into sync directories.

### Restore availability probe (the one retained round-trip)

Restore asks "does anyone still hold the bytes for this soft-deleted file?"
*without* pulling them. Rather than reintroduce a whole-file discovery message,
this is a single `ChunkRequest` for `offset = 0` used as a probe: any
`ChunkData` (or a relayed one) proves availability; exhaustion (`ChunkMiss` from
all directions) proves absence. The probe simply discards the returned bytes.
This keeps one protocol for both "is it there?" and "give it to me".

## What gets deleted

- `Sync::Transfer*` and `Sync::Fetch*` wire variants, and `TransferId` /
  `RequestId` as transfer/fetch correlators.
- `transfer.rs`'s `TransferMessage` open/`Start` handshake and `TransferId`
  demux; `run_sender`'s transfer-session shape becomes a stateless
  `answer_chunk_request` function.
- `fetch.rs`'s `RelayUp` / `fetch_cache` store-and-forward and the whole-file
  `PendingFetch` machinery. It is **replaced** (not kept) by the content-keyed
  relay waiter table, which reuses the same exhaustion/prune logic but no longer
  receives or caches bytes.
- `request_pull_from_origin`, `PeerCommand::StartReceive`, and the
  `ReceiverPurpose::Materialize` vs `Fetch` split (both become "receive to temp,
  then run a terminal action").
- The three-way source resolution in the `TransferStart` handler (sync dir /
  provider / fetch cache) collapses into the serve-side `ChunkRequest` answer
  (verified-hash cache / provider).

## Migration / compatibility

- **There is currently no protocol version on the wire.** The handshake
  (`identity::HandshakeMessage`) carries only `public_key` + `signature` (an
  identity proof); the `Frame`/`Sync`/`Change` types carry no version. Today a
  mismatched-protocol peer would surface only as an rmp deserialize failure and a
  dropped session — no clean "incompatible" signal.
- **This change adds a protocol version to the handshake.** Add
  `protocol_version: u32` to `HandshakeMessage`, set to a new constant (e.g.
  `PROTOCOL_VERSION`), and reject in `Identity::verify_handshake` (new
  `HandshakeError::IncompatibleProtocol { ours, theirs }`) when it does not match
  ours. This gives an explicit, fail-closed cutover for this change and a clean
  gate for every future protocol change.
  - Adding the field itself changes the handshake wire shape, so an old peer and
    a new peer fail to exchange handshakes at all (deserialize error) — which is
    the desired fail-closed behavior for the very first version gate.
  - The signature still covers only the peer's public key (unchanged); the
    version field is advisory metadata checked *after* signature verification, so
    it does not weaken the auth proof. (If desired later, fold the version into
    the signed payload; not required now.)
  - Policy for now: require **exact** equality. Since all devices are operated by
    the same user and updated together, there is no need for a compatibility
    range. Relax to a min-supported range only if mixed-version operation is ever
    wanted — **not planned**.
- `CHUNK_SIZE` becomes part of the wire contract (it defines chunk boundaries and
  the canonical chunk length every node derives). Changing it is a
  protocol-breaking change and must go with a version bump. It is currently
  64 KiB (`transfer.rs`).
- **No database migration.** `file_versions_v1` and the manifest are unchanged;
  this is purely a wire + in-memory-routing change. (Per `AGENTS.md`, a schema
  change would require a `_v2` chain; none is needed here.)

## Testing

- Unit: `transfer.rs` receiver against a stateless chunk-answering stub. Assert:
  a clean multi-chunk receive completes with the correct hash; a `ChunkData`
  duplicated for an already-written offset is ignored; a total `ChunkMiss`
  mid-stream fails the receive immediately (no retry); a per-chunk no-progress
  stall trips the liveness timeout and fails; a second holder answering a chunk
  the first missed lets the receive complete (multi-source on the first request,
  not a re-flood).
- Unit: serve-side verified-hash cache invalidation on mtime/size change; a
  malformed (misaligned or out-of-range `offset`) request answered `ChunkMiss`.
- Unit: relay waiter table — coalescing (two downstreams, one upstream fetch,
  fan-out to both), exhaustion (`ChunkMiss` from all upstreams fans `ChunkMiss`
  down), link-drop pruning, and TTL expiry. Adapt the existing `fetch.rs`
  flood/exhaustion tests to the content-keyed table.
- Integration (three-node harness): reproduce the bug — phone edits, computer
  receives via central relay while central is still catching up; assert the
  computer converges to the new content without a reconnect. Assert relays hold
  no temp file (cut-through) and bounded memory.
- Regression: `tagsy edit` on a file not present locally still fetches via the
  same path; restore availability probe (offset-0 `ChunkRequest`) resolves
  present/absent correctly.

## Resolved parameters

### Failure handling: no retries, one liveness timeout

A receive **never retries and never re-floods after a miss**. Transport is
WebSocket-over-TCP, so on a live connection there is no silent frame loss: a
`ChunkRequest` on an open link is delivered and its reply returns, or the link
itself breaks. Therefore the only failure modes are structural, and none is
helped by retrying the same hash:

- **Total `ChunkMiss`** (every reachable direction missed a chunk): the version
  is superseded (holders moved to a newer `content_hash`) or the only real
  holder sits behind an unreachable relay. Retrying the old hash is futile.
- **Link drop** (the source disconnected): the peer session ends and every
  receive depending on that link fails. Retrying a dead link is meaningless.
- **Liveness timeout** (a *connected* peer accepted the request but went silent —
  a bug/wedge, not packet loss): the one guard worth keeping, purely to avoid
  hanging forever.

On any of these the receive fails immediately. **Recovery is entirely external:**
a newer `FileMetadataChanged` starts a fresh receive for the new hash, and
reconnect → manifest reconcile re-attempts a wanted file whose holder was
offline. There is no per-chunk budget and no retarget counter to tune.

Multi-source is preserved without retries: each chunk's *first* request already
goes to whoever currently holds it (origin/last-good direction, flooding only
when the direction is unknown), so central taking over once it has caught up
happens naturally on the next chunk — not via a re-flood of a missed one.

**Liveness timeout = `HOP_TIMEOUT` (8s), per-chunk, no-progress.** The clock is
reset whenever any chunk is written; if no chunk is written for `HOP_TIMEOUT`,
the receive fails. This adapts to large files (a steadily-progressing transfer
never trips it) without a fixed overall budget. It only ever *fails* the receive;
it never re-requests.

### Relay waiter-table TTL = `HOP_TIMEOUT` (8s)

A relay waiter entry is armed with an 8s TTL when it is created (first upstream
flood). On expiry it fans `ChunkMiss` to its downstream waiters and drops the
entry. The TTL is **not** refreshed when a new downstream coalesces onto an
existing entry (otherwise a stream of joiners could keep an entry alive
indefinitely on a dead upstream). This is the same `HOP_TIMEOUT` constant playing
the same "how long to wait on children" role it plays in `fetch.rs` today, now
keyed by content — one tunable across both the relay layer and the receiver
liveness guard.

### Window unchanged: `WINDOW = 8`

End-to-end in-flight bytes stay `WINDOW * CHUNK_SIZE` (= 512 KiB) per receive per
hop; relay depth does not multiply it. A relay is reactive — it forwards only the
chunk requests it receives and holds at most one waiter entry per distinct
in-flight `(file_id, content_hash, offset)`, so a single receive presents at most
`WINDOW` keys at any hop, mirroring the receiver's window rather than adding its
own. Fan-in (a relay serving K concurrent receives of *different* files) is
bounded by real demand at `K * WINDOW` tiny metadata entries (link-handle sets,
**no byte buffers**); coalescing *reduces* load when receives overlap on the same
file. No per-depth window reduction is needed.

## Confirmed invariants (verify during implementation)

- Coalescing cannot mix versions: the waiter key includes `content_hash`, so a
  single `ChunkData` fanned to multiple downstreams is by construction the same
  version's bytes. An upstream that changed files could only have answered one
  consistent hash.
- Relays hold no byte buffers (assert in the relay unit test).
- A steadily-progressing large-file receive never trips the per-chunk liveness
  timeout (assert with a throttled-but-live stub sender).
