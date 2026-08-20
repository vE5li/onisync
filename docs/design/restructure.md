# Restructuring: module layout, naming, and the FFI error boundary

## Status

Proposed. Work plan, to be executed one item at a time. Each item below is
independently landable and leaves the tree green.

Baseline: `d826f320` ("Support %-delimited regex payloads in search queries"),
plus the `intial_sync_*` → `initial_sync_*` typo fix (item 0.1, done).

Baseline test suite: **204 tests, all passing, 0.92s**
(`cargo test --workspace`). This is the safety net for every mechanical item
below; any item that changes the count should say so explicitly.

## Motivation

The tree grew by expansion rather than design. Four files carry most of it:

| File | Lines | Tests | Largest item |
|---|---|---|---|
| `tagsyd/src/lib.rs` | 6511 | 1013 | `handle_changes` — 2242 lines, one `async fn` |
| `tagsyd/src/database.rs` | 4482 | 1633 | `impl FileDatabase` — 2205 lines, 65 methods |
| `tagsyd/src/directory_manager.rs` | 2655 | 815 | `handle_command` 411 + `handle_event` 266 |
| `tagsyd/src/api.rs` | 1768 | 259 | `impl Api` 1019 + `mod chunk` 508 |

The design underneath is sound — it is a message-passing actor system with a
ports-and-adapters frontend — but the flat `mod` list in `lib.rs` hides it, and
several load-bearing invariants are enforced by convention alone.

## Key insight

**The module boundaries should be the actor boundaries.**

There are four state-owning actors, each with exactly one inbox:

| Actor | Owns | Inbox |
|---|---|---|
| `handle_changes` | the only `&mut FileDatabase` | `DaemonMessage` |
| `SyncDirectoryManager` | the filesystem + per-directory indexes | `SyncDirectoryCommand` |
| peer session tasks | one socket each | `Frame` / `PeerCommand` |
| relay engines | the waiter tables | (shared `Arc`) |

"Only `handle_changes` writes the main database" is the central invariant of the
whole system, and today it is enforced by *nothing*: `FileDatabase` is `pub`
with 54 public methods, two of them `&mut self`. Aligning modules with actors
turns that invariant into something a reader can check from `pub` markers, which
is roughly the only structural guarantee Rust's module system can actually give
us.

The secondary axis is **pure vs. I/O-driving**. Every test in the tree lives in
a pure function (reconcile, query lexer, tag rules, path validation, LWW
semantics); the dispatch and I/O layers have near-zero coverage. Naming the pure
kernels as their own modules makes that split visible.

## Non-goals

- **Collapsing the mirrored API surface.** The API is expressed six times
  (`Api` → `ControlRequest` → `dispatch` → `ControlResponse` →
  `IpcClientBackend` impl → `Backend` forward, plus the CLI and bridge). This is
  a deliberate decision, not an accident: each level is intended to be
  independently useful and independently readable. Do **not** replace it with a
  code-generating macro. If a future reader is tempted, this paragraph is the
  answer.
- **Rewriting behaviour.** Every item below is a move, a rename, or a bug fix
  with a named symptom. No item changes sync semantics, the wire protocol, or
  the database schema.
- **Renaming SQL tables.** Table names are version-suffixed and migrating them
  costs a schema version for zero runtime benefit. Rust-side type renames are in
  scope; `entries_v1` etc. stay.

---

# Target architecture

```
                    ┌──────────────── frontends ────────────────┐
   CLI ──IPC──┐     │                                           │
   Flutter ───┼──> Backend (trait) ──> LocalBackend ──CatalogCommand──┐
   (in-proc) ─┘     │                                                 │
                    └─────────────────────────────────────────────────┼─┐
                                                                      ▼ │
                                    ┌───────────── catalog::Catalog ─────┐
                                    │  the only &mut CatalogStore        │
                                    └──┬───────────────┬─────────────────┘
                        WorkspaceCommand              Frame / PeerCommand
                                    ▼                   ▼
                              Workspace           peer session tasks
                        (filesystem + indexes)    (sockets) ──> relays
```

## Target module layout — `tagsyd`

```
tagsyd/src/
  lib.rs              ~250 lines: mod decls, run(), ShutdownSignal, RunError

  store/                          # was database.rs (4482)
    mod.rs            CatalogStore handle + initialize() + pub(crate) conn accessor
    schema.rs         all create_*_vN / migrate_*_to_vN — the frozen contract, one file
    types.rs          DatabaseError, DeletedRule, SubtagRule, FileVersion, DeletionState
    files.rs          files_v2: CRUD, tombstone LWW, manifest_entries
    tags.rs           tags_v1
    entries.rs        entries_v1: relationships + the three graph walkers
    versions.rs       file_versions_v1
    previews.rs       previews_v1 cache
    query.rs          QueryTerm / TextPattern / CompiledPattern + the two query fns
    short_id.rs       prefix resolve / shorten
    directory_index.rs  was SyncDirectoryDatabase

  catalog/                        # was handle_changes + its orbit
    mod.rs            struct Catalog { .. } + run(rx) + top-level dispatch
    messages.rs       was bus.rs (CatalogCommand, Ingest, ContentChange, PeerCommand)
    content.rs        handle_content_change, apply_tag_rules, place_content
    files.rs          FileMetadata*/Moved/Deleted/Restored + CatalogFile/Materialize
    tagging.rs        Tag* / File(Un)Tagged / TagTag* arms
    placement.rs      Placement, placements_for, plan_placement, apply_placement
    forward.rs        forward_to_peers, WireKind
    previews.rs       resolve_preview, maybe_eager_preview, generate_preview_from_local

  peer/
    mod.rs
    dial.rs           handle_connection, connect_to_peer
    handshake.rs      was identity.rs + read_handshake + HandshakeResult
    session.rs        run_peer_session, PeerContext, send_frame
    plan.rs           plan_file_sync (was reconcile_peer_manifest) + tests
    plan_tags.rs      plan_tag_sync (was reconcile_peer_tag_manifest)
    relay/
      mod.rs          generic waiter table + peer directory + HOP_TIMEOUT
      chunks.rs       ChunkRelay   (was PendingFetches)
      previews.rs     PreviewRelay (was PendingPreviews)
      providers.rs    ProviderRegistry
    transfer/
      mod.rs          CHUNK_SIZE, WINDOW, ChunkRequest/Reply, TransferError
      receive.rs      receive_inner
      serve.rs        answer_chunk_request + VerifiedHashCache
      source.rs       ChunkSource + impls + ProviderSource

  workspace/                      # was directory_manager.rs + watcher.rs
    mod.rs            struct Workspace + run()
    commands.rs       WorkspaceCommand + handle_command arms
    events.rs         handle_event (filesystem-watcher side)
    initial_sync.rs   the two initial_sync_* passes (deduped)
    placement.rs      apply_placement, first_holding_path, resolve_unique_physical
    self_write.rs     SelfWrite echo-suppression tracker
    watch/
      mod.rs          WatchDispatcher
      debounce.rs     Debouncer + DebouncedEventKind

  frontend/
    api/                          # was api.rs
      mod.rs          LocalBackend struct + ctor
      read.rs         resolution / lookup / traversal / search
      write.rs        enqueue-based mutations
      request.rs      oneshot round-trips + one shared timeout helper
      edit.rs         begin/finish/cancel edit
      error.rs        ApiError + From impls
      token.rs        was `mod chunk` — the query lexer
    ipc_server.rs     serve_control + handle_control_connection + dispatch
    in_process.rs     the Backend impl over LocalBackend

  config/                         # was configuration.rs
    mod.rs            Configuration, SyncDirectory, SyncType, PreviewGenerationPolicy
    tag_rules.rs      TagRule / CompiledTagRules
    runtime.rs        RuntimePeer, RuntimeConfiguration   (state, not config)

  preview/                        # was preview.rs (generation only)
    mod.rs  image.rs  pdf.rs  video.rs  text.rs

  content/
    file_bytes.rs
    hash.rs           hash_file, moved out of control.rs
  clock.rs            now_millis, moved out of database.rs
  paths.rs            StorageLayout
```

## Target crate graph

```
tagsy-core     ids, LogicalPath/PhysicalPath, wire protocol, Preview, FileInfo
    ▲
tagsy-api      THE PORT: Backend trait, ApiError, and every DTO crossing it
                 (Tag, DeletedRule, SubtagRule, SearchResults, EditOutcome,
                  ApiEvent, EditorRule, RetagSummary, TagRuleReport, Operation*)
    ▲
tagsy-ipc      ControlRequest/Response/Frame + codec + IpcBackend (client)
    ▲                          ▲
tagsy (CLI) ────┘            │
                          tagsyd  (server half + everything above)
                               ▲
                          tagsy-bridge
```

Today `tagsy` (the CLI) depends on `tagsyd`, which — because
`preview-generation` is a default feature — makes building the CLI compile
`rusqlite`, `tokio-tungstenite`, `image`, `infer`, and `pdfium-render`. Nothing
structurally prevents the CLI from opening the database behind the daemon's
back.

## Target layout — Flutter app

```
app/lib/
  main.dart
  shell/            TagsyAppRoot, global keys, Ctrl+C handler, message host
  session/          TagsySession   (out of bootstrap/ — every screen imports it)
  data/
    repository.dart replaces tagsy_service.dart
  bootstrap/        unchanged
  features/
    search/         home_screen + _SearchBar/_TagRow/_FileRow/_SectionHeader
    file/           file_detail_screen + its transport actions
    tag/            tag_detail_screen + recolor dialog
    operations/     operations_screen + operation_row + kind→(icon,label) table
    share/          share_review_screen
  widgets/          TagChip, PropertyTile, FilePreview, RemotePreview,
                    TagsSection, TagPickerSheet, TextPromptDialog,
                    BusyIconButton, RovingFocusList
  editor/           unchanged
  format/           formatSize, uniqueDestination, nameFor, operation labels
```

---

# Naming decisions

Reference table. Rationale is recorded once here so individual items can just
cite it.

## Collisions — one word, unrelated meanings

| Now | To | Why |
|---|---|---|
| `api::chunk::{Chunk, ChunkKind, lex_one_chunk}` | `token::{Token, TokenKind, lex_one_token}` | "Chunk" already means a 64 KiB byte range across the whole transfer stack (`CHUNK_SIZE`, `ChunkRequest`, `ChunkSource`, …). "Token" is the universal term for a lexer's output unit. Frees `chunk` to mean exactly one thing. |
| `reconcile_peer_manifest` | `plan_file_sync` | "Reconcile" currently means both *compute a delta* (pure, no `.await`) and *apply a delta* (mutating) — precisely the distinction you most want visible in sync code. |
| `reconcile_peer_tag_manifest` | `plan_tag_sync` | as above |
| `reconcile_tag_placement` (×2, in `lib.rs` and `directory_manager.rs`) | `plan_placement` / `apply_placement` | Two same-named functions on opposite sides of a channel. Split by what they do. |
| `DaemonMessage::ReconcilePlacement` | `CatalogCommand::PlanPlacement` | |
| `SyncDirectoryCommand::ReconcileTagPlacement` | `WorkspaceCommand::ApplyPlacement` | |
| `ContentTarget` | `Placement` | `target` / `placement` / `dispatch` are three words for one concept. Standardise on **placement**. |
| `change_targets` | `placements_for` | |
| `dispatch_content_to_sync_directories` | `place_content` | |
| `TransportBackend` (trait) | `Backend` | The shorter name currently belongs to the *enum*, which reads like the more fundamental thing. |
| `Backend` (enum) | `AnyBackend` | `Any-` is the conventional Rust marker for a static-dispatch union. Makes `impl Backend for AnyBackend` read correctly. |
| `InProcessBackend` / `IpcClientBackend` | `LocalBackend` / `IpcBackend` | |
| `EntryType` | **delete**, use `core::state::RelationshipKind` | Identical two-variant enum; `database.rs:273–289` carries `From` impls in both directions purely to translate between two names for the same thing. Move `ToSql`/`FromSql` onto `RelationshipKind`, drop four impl blocks. |
| `QueryEntries` (bridge) / `QueryResult` (api) | `SearchResults` on both | Same payload, two names. Also aligns with the CLI's `search` subcommand and the UI's search box; `run_query` becomes `search`. |
| Rust `TagsyApp` (bridge opaque) | `Tagsy` | Collides with the Flutter widget `TagsyApp` in `app/lib/app.dart`; both are in scope in `file_detail_screen.dart`, disambiguated only by import prefix. The bridge type is an API handle, not an app. |

## Vague words — pattern names instead of responsibilities

| Now | To | Why |
|---|---|---|
| `handle_changes` | `catalog::Catalog` + `Catalog::run()` | It handles `Fetch`, `Restore`, `GetPreview`, `PurgePreviews`, `Materialize`, `CatalogFile`, `AnnounceProvided` — far more than "changes". What it *is* is the authoritative index of what exists and the sole writer to it. The code already reaches for the word (`DaemonMessage::CatalogFile`). |
| `DaemonMessage` | `CatalogCommand` | Every message in a daemon is a daemon message. Naming an inbox after its actor gives a uniform convention (`CatalogCommand` / `WorkspaceCommand` / `PeerCommand`) so a channel's type tells you who is on the other end. |
| `SyncDirectoryManager` | `Workspace` | "Manager" is a pattern, not a responsibility. Also breaks up the `SyncDirectory` / `…Manager` / `…Database` / `…File` / `…Command` / `RichSyncDirectory` pile-up where the prefix has stopped carrying information. |
| `SyncDirectoryCommand` | `WorkspaceCommand` | follows |
| `SyncDirectoryDatabase` | `DirectoryIndex` | Ten methods over a two-column `(file_id, physical_path)` table. Calling it a `Database` implies parity with the 65-method `FileDatabase` and invites people to grow it. |
| `FileDatabase` | `CatalogStore` | It holds files, tags, relationships, versions **and** previews. "File" is wrong. Pairing `Catalog` (actor) with `CatalogStore` (its persistence) makes the ownership relationship legible. |
| `RichSyncDirectory` | `OpenDirectory` | "Rich" is a nothing-prefix; what it adds is an opened database handle. |
| `Api` | `LocalBackend` | Four "api"s in the tree. This struct is specifically the in-process implementation of the port. |
| `PendingFetches` / `PendingPreviews` | `ChunkRelay` / `PreviewRelay` | They route, coalesce and fan out — "relay" is the word their own module docs use. Naming both `*Relay` also makes their ~90% code overlap impossible to unsee. |
| `WantedFile` / `WantedDeletion` / `WantedRestore` / `WantedMove` | `SyncPlan { pulls, deletions, restores, moves }` of `MissingContent` / `PeerDeletion` / `PeerRestore` / `PeerMove` | "Wanted" is backwards for three of four: a `WantedDeletion` is not a deletion we want, it is one the *peer* performed that won LWW and we are obliged to apply. `Peer*` names where the authority came from — the entire content of the LWW decision. |
| `bus.rs` | `catalog/messages.rs` | There is no bus type in the file; it is a message vocabulary, ~85% doc prose. |
| `Operations` (registry) | `OperationRegistry` | Plural-of-type reads like `Vec<Operation>`; this is a live registry with a broadcast channel. |
| `Paths` | `StorageLayout` | Not a generic path utility — specifically *this device's* storage locations. |
| `initial_sync_tagged` | `initial_sync_tag_based` | The enum variant is `SyncType::TagBased`; two words for one policy makes the dispatch read as if they differ. |
| `SpecialType { Upload, Copy }` | **delete** | Declared in `configuration.rs:66`, referenced nowhere. |
| Dart `tagsy_service.dart` | **delete** (see 0.7) | "Service" means nothing, and it is an extension, not a service. |
| Dart `_watch()` (×4 screens) | `_subscribeToChanges()` | Reads like a getter; opens a lifetime-scoped stream subscription. |

## Spelling

Identifiers are American 15:1 (`initialize`, `materialize_to`, `deserialize`,
`finalized`) — and cannot move, since serde forces `Serialize`/`Deserialize`.
Prose is British throughout (`serialises`, `normalises`, `materialises`,
`initialises`, `recognised`, `normalisation`).

**Rule: American everywhere.** A reader should not have to try both spellings
when grepping, and a split convention silently invites new code to pick either.

## Conventions to keep (and write down)

The DTO layering is already correct and deliberate. Do not collapse it:

| Layer | Suffix | Example |
|---|---|---|
| core domain / wire | none, or `-Info` | `FileInfo`, `Tag`, `Preview` |
| FFI DTO | `-Entry` | `FileEntry`, `TagEntry`, `OperationEntry` |
| CLI presentation | `-Row` | `FileRow`, `TagRow` |

Each suffix tells you which boundary you are on and therefore what is safe to
change. This belongs in `AGENTS.md`.

---

# Work plan

Sizes are rough: **S** < 1h, **M** a focused session, **L** a day or more.

## Phase 0 — Standalone fixes — **COMPLETE**

No structural change. Each is independently landable today and none blocks any
other. 0.6 is a live user-visible bug.

### 0.1 — `intial_sync_*` → `initial_sync_*` — **DONE**

Typo, 4 sites in `directory_manager.rs`. A full misspelling scan of the repo
found nothing else.

### 0.2 — Spelling convention: American everywhere — **S** — **DONE**

- **Why** One British identifier (`normalise_id_prefix`) against 15 American
  ones; prose is uniformly British. See *Naming decisions → Spelling*.
- **Change** Rename `database.rs:434` `normalise_id_prefix` →
  `normalize_id_prefix` (+ its ~8 call sites and the `normalised` locals around
  `1271–1346`). Rewrite the ~15 British prose words: `serialise(s)`,
  `normalise(s)`, `normalisation`, `materialise(s)/ing`, `initialises`,
  `recognised`. Also `app/lib/screens/tag_detail_screen.dart` `_normalised` →
  `_normalized`.
- **Verify** `cargo test --workspace` (204 pass); `rg -i 'normalis|serialis|materialis|initialis|recognis'` returns nothing outside `target/`.
- **Depends on** —

### 0.3 — `initial_sync_tagged` → `initial_sync_tag_based` — **S** — **DONE**

- **Why** Mismatch with `SyncType::TagBased`.
- **Change** Rename in `directory_manager.rs` (2 sites).
- **Verify** `cargo check -p tagsyd`
- **Depends on** —

### 0.4 — Delete `SpecialType` — **S** — **DONE**

- **Why** Dead code with a meaningless name (`configuration.rs:66–70`).
- **Change** Delete the enum. Confirm no serde config field references it.
- **Verify** `cargo test --workspace`
- **Depends on** —

### 0.5 — Delete `EntryType`, use `RelationshipKind` — **S** — **DONE**

- **Why** Two names for one two-variant enum, with `From` impls both ways
  (`database.rs:248–289`) purely to translate between them.
- **Change** Move the `ToSql`/`FromSql` impls onto
  `tagsy_core::state::RelationshipKind`. Delete `EntryType` and its four impl
  blocks. Update `entries.rs` call sites (the table name `entries_v1` is
  unchanged).
- **Note** `tagsy-core` already depends on `rusqlite`, so this adds no
  dependency.
- **Verify** `cargo test --workspace`
- **Depends on** —

### 0.6 — Fix the FFI error boundary — **M** — ⚠ live bug — **DONE**

- **Why** `ApiError` crosses FFI as an opaque handle
  (`api.dart:16` → `abstract class ApiError implements RustOpaqueInterface {}`),
  and `RustOpaque` in flutter_rust_bridge 2.12.0 does not override
  `toString()`. So `'$error'` is `Instance of 'ApiErrorImpl'`, and **all seven**
  `.contains('NotFound')` / `.contains('Ambiguous')` checks always evaluate
  false. Current symptoms:
  - `home_screen.dart:188` — typing `$fo` flashes a red error every keystroke,
    which is exactly what the comment above it claims to prevent.
  - `remote_preview.dart:77` — always renders
    "Failed to load preview / Instance of 'ApiErrorImpl'" instead of
    "Preview unavailable / No device that holds this file is currently
    reachable."
  - `file_detail_screen.dart:143`, `tag_detail_screen.dart:131` — the
    self-pop-when-deleted path never fires.
  - `file_detail_screen.dart:294, 338, 409` — three unavailability messages
    never shown.
- **Change**
  1. Make the bridge expose `ApiError` as a *mirrored* enum, not an opaque
     handle, so frb generates a Dart sealed class.
  2. Split the overloaded `NotFound`. Three unrelated conditions collapse into
     it today (`api.rs:71–123`): `DatabaseError::MissingFile|MissingTag`,
     `FetchError::NotAvailable`, `RestoreError::NotAvailable`.

     | Now | To |
     |---|---|
     | `NotFound` (unknown id) | `UnknownId` |
     | `NotFound` (no peer has bytes) | `ContentUnavailable` |
     | `Ambiguous(String)` | `AmbiguousId(String)` |
     | `InvalidArgument`, `Database`, `Transport`, `Internal` | unchanged |
  3. Replace all seven Dart string checks with pattern matches.
- **Verify** `cargo test --workspace`; `nix run .#codegen`; `flutter analyze`;
  manual: type `$fo` in the search box (expect "no matches", not red), and open
  a file whose peer is offline (expect "Preview unavailable").
- **Depends on** —

### 0.7 — Drop `_by_string` / `_string`, delete `tagsy_service.dart` — **M** — **DONE**

- **Why** Two suffixes for one concept (`_string` on 3 methods, `_by_string` on
  12) — but the deeper problem is that the suffix marks the *wrong* method as
  special. On the Dart side there is no such thing as a non-string id:
  `FileEntry.fileId` is already a `String`. The opaque `FileId`/`TagId` handles
  taken by six bridge methods (`bridge/api.rs:438, 449, 454, 765, 820, 855, 860`)
  are unusable from Dart except as tokens handed straight back, and
  `app/lib/tagsy_service.dart` (71 lines) exists solely to paper over them.
- **Change** Change those six methods to take `String`. Drop every `_string` /
  `_by_string` suffix (≈15 Rust methods, ≈25 Dart call sites). Delete
  `app/lib/tagsy_service.dart`.
- **Rule to record** *The bridge speaks Dart's types.* `String` in, DTOs out, no
  Rust handles except genuinely long-lived resources (`Tagsy`,
  subscriptions). If a handle-taking variant must survive, invert the naming:
  `move_file(String)` and `move_file_by_id(FileId)` — never suffix the path
  everyone takes.
- **Verify** `cargo test --workspace`; `nix run .#codegen`; `flutter analyze`;
  `rg -n 'ByString|_by_string|_string\b' app/lib tagsy-bridge` returns nothing.
- **Depends on** — (independent of 0.6, but both need codegen; consider landing
  0.6 first so there is only one codegen churn on `ApiError`)

### 0.8 — Query lexer: `chunk` → `token` — **S** — **DONE**

- **Why** Head-on collision with the transfer protocol's byte chunks. See
  *Naming decisions → Collisions*.
- **Change** `api.rs:1261–1768` `mod chunk` → `mod token`; `Chunk` → `Token`,
  `ChunkKind` → `TokenKind`, `lex_one_chunk` → `lex_one_token`. `lex_query`
  keeps its name. 27 existing tests move with it.
- **Verify** `cargo test --workspace` — 204 pass, count unchanged.
- **Depends on** —

### 0.9 — Move `hash_file` out of `control.rs` — **S** — **DONE**

- **Why** `api.rs:785` calls `crate::control::hash_file`, while `control.rs`
  is built on `api` — a genuine module cycle, hidden from both import blocks
  because it is written as an inline fully-qualified path. `hash_file` is a pure
  streaming BLAKE3 helper with no protocol content.
- **Change** Move `control.rs:1108–1134` → `content/hash.rs` (or
  `file_bytes.rs`). Update the two call sites.
- **Verify** `cargo test --workspace`
- **Depends on** —

### 0.10 — Move `HOP_TIMEOUT` out of `fetch.rs` — **S** — **DONE**

- **Why** Second hidden cycle: `transfer.rs:209, 222` reach into
  `crate::fetch::HOP_TIMEOUT` while `fetch.rs:43` imports from `transfer`.
  `preview_fetch.rs:34` also imports it.
- **Change** Move the const to wherever both can see it without a cycle
  (`transfer/mod.rs` is the natural home once Phase 3 lands; until then a small
  `timeouts.rs`).
- **Verify** `cargo test --workspace`
- **Depends on** —

---

## Phase 1 — `store/` — **COMPLETE**

### 1.1 — Split `database.rs` into `store/` — **L** — **DONE**

- **Why** 4482 lines, 49% one impl block. The seams are unusually clean: seven
  of them are single-table, and the 71 tests already cluster 1:1 onto them.
- **Change** Split per *Target module layout → store/*. Mechanics: expose a
  `pub(crate) fn connection(&self) -> &Connection` on the handle (the
  `create_*` / `migrate_*` / `resolve_id_prefix` functions already take
  `&Connection`), then move methods out as sibling `impl CatalogStore` blocks.
  Move each test cluster with its module; hoist `memory_db`, `file_id_from_hex`,
  `tag_id_from_hex` into a shared `#[cfg(test)] mod fixtures`.
- **Seam map** (verified line ranges in the current file):
  | Module | Source lines | Notes |
  |---|---|---|
  | `previews.rs` | 687–836, tests 4392–4482 | Cleanest. Two external writes to `previews_v1` (`record_version:893`, `remove_file:1553`) must call a shared helper. |
  | `versions.rs` | 639–991 | Only outbound edge is the preview delete above. |
  | `short_id.rs` | 372–447, 1222–1348, inline block 2636–2654, tests 2986–3229 | Already generic over `(table, column)`; `shorten_file_id` and `shorten_tag_id` are near-identical and should collapse. |
  | `query.rs` | 119–246, 1879–2165, tests 3618–3866 | Pure composition, no new SQL. |
  | `entries.rs` | 610–637, 1086–1185, 1687–1851, 2168–2307 | Traversals `LEFT JOIN tags_v1` for the tombstone filter — read-only cross-table edge. |
  | `files.rs` | 480–572, 1001–1054, 1190–1197, 1357–1557, 2374–2657 | |
  | `tags.rs` | 574–608, 1063–1157, 1283–1290, 1570–1677, 2314–2502 | |
  | `directory_index.rs` | 2660–2848 | Zero coupling; moves verbatim today. |
  | `types.rs` | 54–117, 305–367 | |
- **Also** Rename `FileDatabase` → `CatalogStore`, `SyncDirectoryDatabase` →
  `DirectoryIndex`. Move `now_millis` (356–367) to `clock.rs` — it is a clock,
  not persistence.
- **Verify** `cargo test --workspace` — 204 pass, count unchanged.
- **Depends on** 0.2, 0.5
- **As landed** — two deliberate departures from the above:
  - **No `connection()` accessor.** Rust already lets a descendant module
    reach a private field of an ancestor's struct, so the per-table modules
    use `self.connection` directly. A `pub(crate)` accessor would have been
    *wider* than the private field — crate-visible rather than
    `store`-subtree-visible — which cuts against the encapsulation this item
    exists to buy.
  - **`create_files_v1` (per-directory) → `create_directory_files_v1`.**
    Both databases' DDL now share one `schema.rs`, where an unprefixed
    `create_files_v1` would sit beside `migrate_files_to_v2` — which reads
    the *main* catalog's unrelated `files_v1`. The prefix keeps the two
    apart; `AGENTS.md` records it as the convention to follow if that table
    is ever versioned.

### 1.2 — Reconcile `AGENTS.md` with reality — **S** — **DONE**

- **Why** The schema section is now partly wrong, and Phase 1 moves the
  functions it tells you to grep for.
  - It names `SyncDirectoryDatabase::migrate_files_to_v2`, which **does not
    exist**.
  - It omits `previews_v1` from the table inventory (a newer table with no
    migration function).
  - Its "grep for `_v1`" discovery mechanism becomes "open `store/schema.rs`".
  - It contains a typo: "`<tabli>_vN+1`".
- **Change** Rewrite the *Database schema versioning* section against
  `store/schema.rs`. Add the module map and the DTO suffix convention.
- **Verify** read-through
- **Depends on** 1.1

---

## Phase 2 — Drain the pure logic out of `lib.rs`

### 2.1 — Extract `peer/plan.rs` and `peer/plan_tags.rs` — **M** — **DONE**

- **Why** The cleanest seam in the file. `reconcile_peer_manifest`
  (`lib.rs:1887–2128`) plus its helpers has **zero `.await`, zero channels, zero
  locks** — it takes only a `&FileDatabase` and a `Vec<ManifestEntry>`. Its 351
  lines of tests move with it for free.
- **Change** Move `build_local_manifest` (1790), the four `Wanted*` structs
  (1825–1885), `reconcile_peer_manifest` (1887), `ReconcileDecision` (2130),
  `decide_request` (2139) and `mod reconcile_tests` (5499–5849). Rename per the
  table: `plan_file_sync` returning a single `SyncPlan`. Do the same for the tag
  side (2713–2905) into `plan_tags.rs`.
- **Verify** `cargo test --workspace`; `lib.rs` drops ~600 lines.
- **Depends on** 0.2
- **As landed** — `lib.rs` drops 971 lines (more than estimated: the tag-side
  move carries its own weight, and the two `effective_placement_tags` tests
  that lived inside `reconcile_tests` moved with the rest of the module,
  referenced as `crate::effective_placement_tags` since that function stays in
  `lib.rs` until 2.2). A `peer` module didn't exist yet (Phase 3 hasn't
  landed), so this item creates `tagsyd/src/peer/mod.rs` declaring just
  `plan` and `plan_tags`; the session machinery joins them in 3.1. Also
  dropped a pre-existing broken intra-doc link on `build_tag_request_response`
  (`[`build_request_response`]` — no such function exists in the tree).

### 2.2 — Extract `catalog/placement.rs` — **M** — **DONE**

- **Why** Highest-value extraction by call count: reached from five arms of
  `handle_changes` (4149, 4346, 5335, 5395) plus `run_peer_session`.
  `contains_all_tags` is called from five places spanning three would-be
  modules — and is **duplicated verbatim** in `directory_manager.rs:20–24`.
- **Change** Move `ContentTarget` (64, 96), `contains_all_tags` (2926),
  `effective_placement_tags` (2932), `DeferredPlacement` (2951),
  `reconcile_tag_placement` (2967), `fetch_and_place_deferred` (3047), plus the
  currently-nested `change_targets` (3793) and
  `dispatch_content_to_sync_directories` (3368). Apply the placement naming from
  the table. Delete the duplicate in `directory_manager.rs` and import.
- **Verify** `cargo test --workspace`
- **Depends on** 2.1
- **As landed** — `catalog` didn't exist yet (like `peer` in 2.1, it's created
  ahead of Phase 4 with just `mod.rs` declaring `placement`). Only the lib.rs
  side of the `reconcile_tag_placement` pair is renamed here, to
  `plan_placement`; the `directory_manager.rs` one keeps its name until it
  becomes `apply_placement` in 5.1 (moving both together would conflate this
  item with that phase's rename). `change_targets` (→ `placements_for`) and
  `dispatch_content_to_sync_directories` (→ `place_content`) were nested `fn`
  items inside `handle_changes`; both close over nothing (confirmed by 4.1's
  own notes), so they lift to module scope with no signature changes — this
  item does that lift now rather than waiting for 4.1. The two
  `effective_placement_tags` tests that had temporarily landed in
  `peer::plan::tests` in 2.1 (referenced via `crate::effective_placement_tags`)
  moved on to `catalog::placement::tests` with the function; three new tests
  for `contains_all_tags` were added there too (it had none before, in any of
  its three call sites). Test count: 204 → 207.

### 2.3 — Extract `catalog/previews.rs` — **S** — **DONE**

- **Why** Removes **every** `#[cfg(feature = "preview-generation")]` from the
  rest of `lib.rs` (six items, including a duplicated
  `try_serve_generated_preview` pair at 2453 / 2494).
- **Change** Move 2240–2507 plus `maybe_eager_preview`.
- **Verify** `cargo test --workspace`; `cargo check -p tagsyd --no-default-features`
- **Depends on** —

### 2.4 — Split `preview.rs` into `preview/` — **S** — **DONE**

- **Why** Three independent backends, each `fn(&[u8]) -> Option<Preview>` with
  no shared state, plus a separately-testable classifier. `pdf` and `video` are
  the only items pulling heavyweight/optional deps.
- **Change** `mod.rs` (generate + classify, 63–171), `image.rs` (177–226),
  `pdf.rs` (239–356), `video.rs` (372–530), `text.rs` (533–550). 12 tests
  distribute across them.
- **Verify** `cargo test --workspace`
- **As landed** — line ranges above were stale (the earlier `FileBytes`-based
  `generate`/`read_source_bounded`/video-path work had grown the file to 822
  lines); mapped against the current file instead, caps/`Kind`/`classify` are
  `pub(super)`, and `mod.rs` refers to the `image` *crate* as `::image` to avoid
  shadowing by the new `image` submodule; 207 tests, count unchanged.
- **Depends on** —

---

## Phase 3 — `peer/`

### 3.1 — Move the session machinery — **M**

- **Change** `handle_connection` (570), `connect_to_peer` (632),
  `HandshakeResult` (778), `read_handshake` (783), `run_peer_session`
  (829–1743), `send_frame` (1745), `clear_outbound_if_owned` (1762),
  `PeerContext` (121), `ReceiverPurpose` (81) → `peer/dial.rs` +
  `peer/session.rs`. Fold `identity.rs` into `peer/handshake.rs`.
- **Verify** `cargo test --workspace`
- **Depends on** 2.1, 2.2

### 3.2 — Split `transfer.rs` into `peer/transfer/` — **S**

- **Why** Four clusters that never call each other; receiver and holder sides
  are fully disjoint.
- **Change** `mod.rs` (50–116), `receive.rs` (133–317), `source.rs` (324–400),
  `serve.rs` (402–578). 11 tests distribute.
- **Verify** `cargo test --workspace`
- **Depends on** 0.10

### 3.3 — Unify the two relays — **M**

- **Why** `preview_fetch.rs` is ~90% `fetch.rs` with `Preview` substituted for
  `Vec<u8>` and the offset dropped — including copy-pasted test fixtures
  (`engine()`, `engine_with_peers()`, the whole `Configuration` literal).
  `PeerOutbound`, `connected_peers`, `peer_outbound`, `prune_link`, `arm_ttl`
  and `fan_miss` are duplicated near-verbatim. `preview_fetch` also has **no
  logging** where `fetch` is densely instrumented.
- **Change** Extract a generic waiter table and a shared peer-directory helper
  into `peer/relay/mod.rs`; reduce `chunks.rs` and `previews.rs` to the
  type-specific parts. Rename `PendingFetches` → `ChunkRelay`,
  `PendingPreviews` → `PreviewRelay`. Share the test `Configuration` fixture.
- **Expected** ~250 lines deleted; 11 relay tests preserved.
- **Verify** `cargo test --workspace`
- **Depends on** 3.2

### 3.4 — Test the handshake — **S**

- **Why** `identity.rs` has **zero tests**, and `verify_handshake:143–182`
  encodes a security property in its ordering: signature verification happens
  *before* the protocol-version gate (173–179). Nothing asserts that.
- **Change** Add tests for sign/verify round-trip, wrong-peer-key rejection,
  malformed base64, wrong key length, version mismatch, and the ordering
  property.
- **Verify** `cargo test --workspace` — count increases.
- **Depends on** 3.1

---

## Phase 4 — `catalog/`

### 4.1 — `handle_changes` → `Catalog` — **L**

- **Why** The hard one, and the reason 1–3 come first. 2242 lines, one `async
  fn`, ten nested items. Eight of the ten (`version_origin`, `forward_to_peers`,
  `dispatch_content_to_sync_directories`, `apply_tag_rules`,
  `handle_content_change`, `change_targets`, `WireKind`, `dispatch_and_forward`)
  **close over nothing** — they already take every dependency as an explicit
  parameter, so they lift to module scope with no signature changes. That is the
  mechanical enabler.
- **Change**
  1. Introduce `struct Catalog { store, workspace_tx, runtime_config, relays, … }`
     with `async fn run(self, rx)` and `async fn handle(&mut self, cmd)`.
  2. Lift the eight closure-free nested items to module scope.
  3. Split the arms across `catalog/{content,files,tagging,forward}.rs` as
     sibling `impl Catalog` blocks:
     - `content.rs` — `handle_content_change` (3469–3791), `apply_tag_rules`,
       `place_content`
     - `files.rs` — `CatalogFile` / `Materialize` / `AnnounceProvided` (4363–4653)
       and `FileMetadata*` / `Moved` / `Deleted` / `Restored` (4687–5196)
     - `tagging.rs` — 5202–5482, already nearly self-contained: only touches
       `store.*_tag*`, `plan_placement`, and `forward_to_peers`
     - `forward.rs` — `forward_to_peers` (3331), `WireKind` (3825–3872),
       `dispatch_and_forward` (3874)
  4. Rename `bus.rs` → `catalog/messages.rs`, `DaemonMessage` →
     `CatalogCommand`.
- **Note** `mod tag_rule_tests` (5851–6511, 15 `#[tokio::test]`) boots the whole
  stack. It uses only `pub`-reachable machinery plus `handle_changes` itself, so
  it should become an integration test under `tagsyd/tests/`.
- **Verify** `cargo test --workspace` — 204 pass, count unchanged.
- **Depends on** 1.1, 2.1, 2.2, 2.3, 3.1

### 4.2 — Trim `lib.rs` to the runtime — **S**

- **Change** What remains: `ShutdownSignal` (154), `RunError` (188),
  `enqueue_declared_tags` (210), `run` (266), `handle_sync_directories` (2907),
  and the `mod` declarations. Target ~250 lines.
- **Verify** `cargo test --workspace`; `wc -l tagsyd/src/lib.rs`
- **Depends on** 4.1

---

## Phase 5 — `workspace/`

### 5.1 — Split `directory_manager.rs` — **L**

- **Change** Per *Target module layout → workspace/*. `SyncDirectoryManager` is
  already a struct, so this is sibling `impl` blocks. Rename to `Workspace`,
  `SyncDirectoryCommand` → `WorkspaceCommand`, `RichSyncDirectory` →
  `OpenDirectory`.
- **Seam map**: `handle_command` (1104–1514, 7 arms with almost no shared local
  state) → one method per command in `commands.rs`; `handle_event` (1516–1781)
  → `events.rs`, with the `Move` arm (1558–1740, 183 lines, three disjoint
  sub-cases: intra / out / in) as its own function; `resolve_unique_physical`
  (583–647) is a pure function of `(index, base, file_id)`;
  `SelfWrite` + `record_self_write` + `take_matching_self_write` (142–151,
  263–299) → `self_write.rs` (its own `TODO: Make this a more robust messaging
  framework` at 159 already flags it).
- **Verify** `cargo test --workspace` — 15 tests preserved.
- **Depends on** 2.2 (for the deduped `contains_all_tags`)

### 5.2 — Dedupe the initial sync — **M**

- **Why** `initial_sync_universal` (649–767) and `initial_sync_tag_based`
  (769–891) are ~120-line near-clones differing only in path derivation
  (`file_id` vs `physical_path`), the untracked-detection key, and
  `upload_file` vs `add_file`. Neither has a direct test.
- **Change** Collapse to one pass parameterised over those three points. Add
  tests first.
- **Verify** `cargo test --workspace` — count increases.
- **Depends on** 5.1, 0.3

### 5.3 — Split and test the watcher — **S**

- **Why** `push_raw` (92–319) contains a seven-rule event-coalescing table —
  the most logic-dense, **entirely untested** code in the tree, carrying its own
  `TODO: Maybe this matching here is too eager` at 205. It is a pure state
  machine over a `Vec`.
- **Change** `watch/mod.rs` (dispatcher + lifecycle, 338–405), `watch/debounce.rs`
  (event vocabulary + predicates 10–78, merge engine 86–336). Split `push_raw`
  at 167 into *translate* (98–165) and *coalesce* (167–318). Add one test per
  merge rule.
- **Verify** `cargo test --workspace` — count increases.
- **Depends on** 5.1

### 5.4 — Make the fake `Result`s honest — **S**

- **Why** `send_change` / `send_content_change` return
  `Result<(), SyncDirectoryError>` but can never fail (they `let _ =` the send),
  propagating a meaningless `?` through ~10 callers. Separately,
  `SyncDirectoryError` is a flat, lossy enum (no source error carried) that is
  private yet appears in the return type of widely-used methods.
- **Change** Drop the `Result` from the two senders. Give the error type
  `#[source]` fields.
- **Also worth auditing here** the panics on the sole workspace thread:
  `directory_manager.rs:720, 739, 887, 930, 1157, 1300, 1302`. A panic there
  takes down all sync-directory handling. Out of scope for a rename, but record
  a follow-up.
- **Verify** `cargo test --workspace`
- **Depends on** 5.1

---

## Phase 6 — `frontend/` and `config/`

### 6.1 — Split `api.rs` into `frontend/api/` — **M**

- **Why** `impl Api` is 1019 lines / 41 methods with three distinct shapes:
  sync reads over a short-lived read handle, sync fire-and-forget writes via
  `enqueue`, and async `oneshot` round-trips. The five round-trip methods
  (`restore_file`, `fetch_file`, `get_preview`, `purge_previews`,
  `local_path_for_file`) share an identical copy-pasted
  `timeout / Ok(Ok) / Ok(Err) / Err(_elapsed)` block.
- **Change** Split per the layout; extract the timeout helper. Rename `Api` →
  `LocalBackend`, `run_query` → `search`, `QueryResult` → `SearchResults`.
- **Verify** `cargo test --workspace`
- **Depends on** 0.8, 0.9, 1.1

### 6.2 — Split `configuration.rs` into `config/` — **S**

- **Why** Three unrelated concerns in one file. `RuntimePeer` /
  `RuntimeConfiguration` (541–592) are mutable session state, not configuration,
  and are the **only** reason `configuration.rs` depends on `crate::bus` and
  `tagsy_core::state::Frame` — extracting them makes the config module a pure
  leaf. The tag-rule engine (192–348) holds a `regex::Regex`, owns 8 of the 13
  tests, and is the cleanest extraction.
- **Change** `mod.rs` / `tag_rules.rs` / `runtime.rs`.
- **Also** `Configuration::new` (panicking, two `TODO: Return a result`) and
  `from_str` / `from_file` (fallible) are a duplicated pair; converge them.
- **Verify** `cargo test --workspace`
- **Depends on** —

### 6.3 — Split `control.rs` — **M**

- **Why** Server and client halves in one file, joined only by the four-function
  codec (832–855). 33 of 35 `dispatch` arms are the identical
  `match api.x(..) { Ok => Response, Err => Error }` shape; 33 of 35
  `TransportBackend` methods are one-line `call` + `match` wrappers. This
  mirroring is intentional (see *Non-goals*) — the split is about locating it,
  not removing it.
- **Change** `protocol.rs` (66–341 + codec 832–855), `server.rs` (343–823),
  `client.rs` (857–1562). Move `read_provider_chunk` (877) — currently sitting
  between the `IpcClientBackend` struct and `IpcClientInner` — next to the rest
  of the provider sub-protocol.
- **Verify** `cargo test --workspace`
- **Depends on** 0.9, 6.1

### 6.4 — `Backend` / `AnyBackend` rename — **S**

- **Why** See *Naming decisions*. Also fixes a real latent bug: `restore_tag`
  and `delete_tag` are **swapped** in the `impl TransportBackend for Backend`
  block (`transport.rs:785, 792`) relative to both the trait declaration and the
  `InProcessBackend` impl — exactly the failure mode hand-written forwarding
  produces.
- **Change** trait `TransportBackend` → `Backend`; enum `Backend` →
  `AnyBackend`; `InProcessBackend` → `LocalBackend`, `IpcClientBackend` →
  `IpcBackend`. Reorder the `AnyBackend` impl to match the trait declaration
  order and add a comment saying it must stay in that order.
- **Verify** `cargo test --workspace`; `nix run .#codegen`
- **Depends on** 6.3

### 6.5 — Split the CLI — **M**

- **Why** 1424 lines, **zero tests**, and a 459-line 21-arm `run`. The
  formatting layer (97–364, ~265 lines) is fully synchronous and backend-free —
  it is the only part that could be unit-tested today and none of it is.
- **Change** `output.rs` (row DTOs, tables, emitters, operation labels),
  `enrich.rs` (the N+1 `tags_by_file` / `tags_by_tag` fan-out and the memoizing
  name cache), `commands.rs` (clap surface), `run.rs` (dispatch), `flows.rs`
  (`edit_file`, `download_file`, `open_in_editor`). Add tests for `output.rs`.
- **Also fix while here**
  - 13 subcommand doc comments reference **`list-files`** and **`list-tags`**,
    neither of which exists; `search` is the only listing path. Same in the
    `file_table` / `tag_table` doc comments (143, 183).
  - `download_file` writes to the current directory, contradicting its own help
    text (578–579) and doc comment (1322–1324).
  - The `EXDEV` fallback (1382–1391) falls back on *any* rename error; it has a
    self-documented TODO.
  - Eleven inline `match output_mode { Human => println!, Json => print_json }`
    blocks (924, 936, 949, 962, 1047, 1060, 1073, 1161, 1203, 1305, 1402) should
    go through one `emit_scalar` helper.
- **Verify** `cargo test --workspace` — count increases.
- **Depends on** 6.4

---

## Phase 7 — Crate split

### 7.1 — Extract `tagsy-api` — **M**

- **Why** `TransportBackend` exists precisely to be the frontend-facing
  contract; putting it inside the daemon crate inverts the dependency. Moving it
  out makes "the CLI cannot touch daemon internals" a compiler-enforced fact.
  It also gives `Tag` / `DeletedRule` / `SubtagRule` a home that is not
  `database.rs` — today they leak from a persistence module onto the wire *and*
  into the CLI, the single leakiest coupling in the tree.
- **Change** New crate holding: the `Backend` trait, `ApiError`, `Tag`,
  `DeletedRule`, `SubtagRule`, `SearchResults`, `EditOutcome`, `ApiEvent`,
  `EditorRule`, `RetagSummary`, `TagRuleReport`, `Operation` / `OperationKind` /
  `OperationStatus` / `OperationEvent`, `EventStream`, `OperationStream`.
- **Verify** `cargo test --workspace`; `nix run .#codegen`
- **Depends on** 6.4, 0.6

### 7.2 — Extract `tagsy-ipc` — **M**

- **Change** Protocol types + codec + `IpcBackend` (client) move out;
  `serve_control` (server) stays in `tagsyd` and depends on the new crate.
- **Verify** `cargo test --workspace`
- **Depends on** 7.1, 6.3

### 7.3 — Cut the CLI's dependency on `tagsyd` — **S**

- **Why** The payoff. `tagsy/Cargo.toml` drops `tagsyd`, which stops the CLI
  build from compiling `rusqlite`, `tokio-tungstenite`, `image`, `infer` and
  `pdfium-render`.
- **Change** Repoint `tagsy` at `tagsy-api` + `tagsy-ipc` + `tagsy-core`.
- **Verify** `cargo build -p tagsy`; `cargo tree -p tagsy` shows none of the
  five heavy deps.
- **Depends on** 7.2, 6.5

---

## Phase 8 — Flutter app

Independent of Phases 1–7 except 8.1, which needs 0.7. Can run in parallel.

### 8.1 — `data/repository.dart` — **S**

- **Change** Replace the deleted `tagsy_service.dart` with a thin repository
  that every screen talks to instead of importing `rust/api.dart` directly.
  Today there is no layer between the widgets and the bridge.
- **Also** Move `TagsySession` out of `bootstrap/` into `session/` — all five
  screens import the bootstrap layer solely for that type.
- **Depends on** 0.7

### 8.2 — Extract the shared widgets — **M**

Ranked by duplication removed:

| Widget | Replaces |
|---|---|
| `TagPickerSheet({allowCreate})` | 3 copies of the whole-store-scan + modal sheet (`file_detail:219–235`, `tag_detail:223–241`, `share_review:69–104`), all carrying the same TODO. The share_review one is richest (has inline creation) and should be the basis. |
| `TextPromptDialog` | 3 near-verbatim "single TextField → pop string" dialogs (`file_detail:800–840`, `tag_detail:551–591`, `share_review:279–316`) ≈120 lines |
| `TagsSection` | Already extracted in `tag_detail:473–547`; `file_detail:654–688` and `share_review:184–223` re-implement it inline. Promote to `widgets/`. |
| `BusyIconButton` | The four AppBar actions in `file_detail:553–627` (~75 lines) repeat one `_flag ? SizedBox(CircularProgressIndicator) : Icon` pattern |
| `RovingFocusList` | The ~120-line bespoke keyboard-navigation system in `home_screen` (`_rowFocus`, `_focusedRowIndex`, `_focusNextRow`, `_focusPreviousRow`, `_ensureRowVisible`, `_restoreRowFocus`) |

- **Verify** `flutter analyze`; `flutter test`
- **Depends on** —

### 8.3 — `features/` reorganisation — **M**

- **Change** Move the eight private widgets at the bottom of `home_screen.dart`
  (555–942, **41% of the file**) out. Highest value: `_OperationsButton`
  (727–870, 144 lines) owns its own operation-stream subscription and is
  completely independent of `HomeScreen` — and its `_watch` (777–817) duplicates
  the stream loop in `operations_screen.dart:76–96`.
- **Depends on** 8.2

### 8.4 — `format/` and the operation label table — **S**

- **Why** `_OperationRow` (`operations_screen:136–246`) is 111 lines of which 76
  are pure string/icon mapping in four parallel switches — and those switches
  mirror `flatten_kind` in `tagsy-bridge/src/api.rs:302–340` across the FFI
  boundary with no sync mechanism. Silent fallthrough to `Icons.pending` if they
  drift.
- **Change** One `const Map<String, (IconData, String)>` with a test asserting
  it covers every kind the bridge can emit. Move `_formatSize`
  (`file_detail:718`), `_uniqueDestination` (`file_detail:520`) and `_nameFor`
  (`share_review:49`, also inlined at `file_detail:323, 389, 463`) into
  `format/`. All are `static` and dependency-free.
- **Depends on** 8.3

### 8.5 — Reconcile the two extension tables — **S**

- **Why** `preview.rs:121–138` (`classify_by_extension`, remote) and
  `file_preview.dart:21–27` (local) are independent tables with different
  contents and no sync mechanism, so a file can be previewable one way and not
  the other.
- **Change** Make the Rust table authoritative and expose it over the bridge, or
  at minimum add a test asserting they agree.
- **Depends on** 2.4

### 8.6 — `TagsyApp` collision — **S**

- **Change** Rename the bridge type to `Tagsy`; rename the Flutter widget to
  `TagsyAppRoot`. Rename the `_app` fields on the screens to `_client`.
- **Depends on** 8.1

---

# Verification recipe

Per item, in order of cost:

```
cargo fmt --all                       # rustfmt.toml is opinionated; run it
cargo check --workspace
cargo clippy --workspace --all-targets
cargo test --workspace                # baseline: 204 pass, 0.92s
cargo check -p tagsyd --no-default-features   # the preview-generation gate
```

For anything touching Dart:

```
jj file list | rg '\.dart$' | xargs dart format
cd app && flutter analyze && flutter test
```

The tracked Dart sources are `dart format`-clean against the Flutter SDK pinned
in `flake.nix`; keep them that way so a formatting pass never lands inside a
behavior change. Format only tracked files — `app/lib/rust/` is codegen output
and is gitignored.

For anything touching `tagsy-bridge` or the FFI surface, also:

```
nix run .#codegen
```

Rules of engagement:

- **Every item lands green.** A move item must not change the test count; state
  it in the commit message when it does.
- **Move first, rename second, refactor third** — as separate commits where an
  item does more than one. A commit that both moves 400 lines and changes their
  content is unreviewable.
- Use `jj` for all version control (see `AGENTS.md`).

---

# Deferred / follow-ups

Identified during analysis, not scheduled:

- **Workspace-thread panics** — `directory_manager.rs:720, 739, 887, 930, 1157,
  1300, 1302`. A panic on the sole sync-directory thread takes down all
  directory handling.
- **`run`'s shutdown-safety doc is already violated.** `directory_manager.rs`
  1783–1806 states "DANGER: do not introduce an `.await` inside
  `handle_command` / `handle_event`", but both are `async fn` and both `.await`
  (`get_file_content`, `content.hash()`, `materialize_to`). Either the doc or
  the code is wrong; decide which.
- **`new` silently drops sync directories** whose database fails to open
  (`directory_manager.rs:172–235`, via `filter_map`).
- **Stale doc comment** — `transfer.rs:726–729` describes a multi-source
  scenario that `hash_mismatch_rejected` (731) does not test.
- **`FileBytesError::Io { path: Some(path.clone()), source }`** is constructed
  nine times in `file_bytes.rs`; a private helper removes ~40 lines.
- **`file_bytes.rs` test helper never cleans up its temp dirs** (`temp_dir`, 364).
- **`service.rs` lock incantation** — `slot().lock().unwrap_or_else(|p| p.into_inner())`
  appears seven times in `tagsy-bridge/src/service.rs`; one
  `with_runtime<R>(f)` helper collapses it.
- **`RuntimeHandle::stop` and `impl Drop`** are verbatim duplicates
  (`runtime.rs:144–149`, `152–161`).
- **`bootstrap_on_disk_state`** (`runtime.rs:170–207`) is pure filesystem +
  `identity`, directly unit-testable against a temp dir, untested.
- **`main.dart` imports both bootstraps unconditionally**, so
  `receive_sharing_intent` and `share_review_screen` are compiled into the Linux
  build.
- **`app.dart:70–91`** — bootstrap failure only `debugPrint`s (has a TODO).
- **`api.rs::parse_query`** silently drops the `/p` (physical path) prefix.
- **`ApiEvent` is still an opaque handle** and is the last data type on the Dart
  surface that is (the other three — `TagsyApp`, `EventSubscription`,
  `OperationSubscription` — are deliberately opaque resources). Dart cannot read
  it, so every screen's change-stream loop reloads its entire state on *any*
  change anywhere. Mirroring it would let a screen filter by the affected
  file/tag id. Same treatment as `ApiError` in 0.6.
