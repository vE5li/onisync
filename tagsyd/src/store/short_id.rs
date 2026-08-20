//! Short ids: the shortest unique prefix of an id, and the inverse lookup that
//! turns a prefix a user typed back into a full id.
//!
//! Generic over `(table, column)` so files and tags share one implementation.

use rusqlite::{Connection, OptionalExtension};
use tagsy_core::{FileId, TagId};

use super::CatalogStore;
use super::types::DatabaseError;

/// Number of leading characters two strings share.
///
/// Operates on `char`s; ids are ASCII hex so this is equivalent to bytes.
pub(super) fn common_prefix_length(a: &str, b: &str) -> usize {
    a.chars().zip(b.chars()).take_while(|(x, y)| x == y).count()
}

/// Outcome of resolving a short-id prefix against an id column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrefixResolution {
    /// Exactly one id starts with the prefix.
    Unique(String),
    /// No id starts with the prefix.
    NotFound,
    /// More than one id starts with the prefix; resolution is ambiguous.
    Ambiguous,
}

/// Resolve a short-id `prefix` against `column` of `table`, returning whether
/// it identifies exactly one row.
///
/// This is the generic counterpart to shortening: given the fewest leading hex
/// characters a user typed, find the full id — or report that the prefix is
/// unknown or ambiguous. It backs every "accept a short id" command, so it is
/// deliberately id-type-agnostic (callers wrap it with a typed helper such as
/// [`CatalogStore::resolve_file_id_prefix`]).
///
/// Ids are stored in canonical simple-hex form, so a prefix match is a plain
/// string-prefix test. We fetch up to two matches: zero → not found, one →
/// `Unique`, two → `Ambiguous`. With the primary-key index on the id column
/// this is a bounded index range scan (`LIMIT 2`), not a full-table scan.
///
/// `prefix` **must** be validated as lowercase hex by the caller (see
/// [`normalize_id_prefix`]); this keeps the `LIKE` pattern free of `%`/`_`
/// wildcards and the query injection-safe (the prefix is still bound as a
/// parameter; `table`/`column` are internal constants, never user input).
fn resolve_id_prefix(
    connection: &Connection,
    table: &str,
    column: &str,
    prefix: &str,
) -> Result<PrefixResolution, DatabaseError> {
    let pattern = format!("{prefix}%");
    let mut statement = connection.prepare(&format!(
        "SELECT {column} FROM {table} WHERE {column} LIKE ?1 ORDER BY {column} LIMIT 2"
    ))?;
    let matches: Vec<String> = statement
        .query_map([&pattern], |row| row.get::<_, String>(0))?
        .collect::<Result<_, _>>()?;

    match matches.as_slice() {
        [] => Ok(PrefixResolution::NotFound),
        [only] => Ok(PrefixResolution::Unique(only.clone())),
        _ => Ok(PrefixResolution::Ambiguous),
    }
}

/// Normalize a user-supplied id or short-id into the canonical lowercase-hex
/// form used for prefix matching.
///
/// Accepts hyphenated UUIDs, full simple-hex ids, and short prefixes of either.
/// Hyphens are stripped (so a pasted full UUID resolves) and the result is
/// lowercased. Returns `None` if any remaining character is not a hex digit —
/// this both rejects junk early and guarantees the value is safe to splice into
/// a `LIKE` pattern (no wildcards).
pub fn normalize_id_prefix(input: &str) -> Option<String> {
    let cleaned: String = input
        .chars()
        .filter(|character| *character != '-')
        .collect();
    if cleaned.is_empty()
        || !cleaned
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        return None;
    }
    Some(cleaned.to_ascii_lowercase())
}

/// Shortest prefix of `full` that no other value in `table.column` shares.
///
/// The trick that makes this scale: a value only ever needs to be
/// distinguished from its two lexicographic *neighbours* (the value
/// immediately before and after it, sorted). If a prefix separates you from
/// both neighbours, it separates you from everyone. So this is two indexed
/// range lookups against the column's primary-key index — O(log n) — not a
/// scan.
///
/// Ids are stored in canonical simple-hex form, so lexicographic ordering on
/// the stored strings is a clean hex ordering and prefixes never straddle a
/// separator. Returns the full length when `full` has no neighbours.
///
/// `table` / `column` are internal constants, never user input.
fn shortest_unique_prefix_length(
    connection: &Connection,
    table: &str,
    column: &str,
    full: &str,
) -> Result<usize, DatabaseError> {
    // Immediate lexicographic predecessor, if any.
    let predecessor: Option<String> = connection
        .query_row(
            &format!(
                "SELECT {column} FROM {table} WHERE {column} < ?1 ORDER BY {column} DESC LIMIT 1"
            ),
            [&full],
            |row| row.get(0),
        )
        .optional()?;

    // Immediate lexicographic successor, if any.
    let successor: Option<String> = connection
        .query_row(
            &format!(
                "SELECT {column} FROM {table} WHERE {column} > ?1 ORDER BY {column} ASC LIMIT 1"
            ),
            [&full],
            |row| row.get(0),
        )
        .optional()?;

    // The prefix must be one longer than the longest prefix we share with
    // either neighbour, so that it excludes both of them.
    let mut required = 0;
    for neighbour in [predecessor, successor].into_iter().flatten() {
        let shared = common_prefix_length(full, &neighbour);
        required = required.max(shared + 1);
    }

    Ok(required.clamp(1, full.len()))
}

impl CatalogStore {
    /// Compute the shortest unique prefix of `file_id` among **all** files in
    /// the database — the "short id" shown in listings, à la `jj`/`git`.
    ///
    /// The result is the fewest leading hex characters of `file_id` that no
    /// other file's id shares; see `shortest_unique_prefix_length` for why
    /// this costs two indexed lookups rather than a scan.
    ///
    /// Note: the returned length reflects the database *at call time*. It is
    /// not stored and not stable across concurrent inserts — a prefix that
    /// is unique now may become ambiguous if a colliding file is added
    /// later. That is the intended behavior (resolution re-checks
    /// uniqueness on use).
    ///
    /// Returns the full id length if the file has no neighbours (e.g. it is the
    /// only file). Returns `MissingFile` if `file_id` is not in `files`.
    pub fn shorten_file_id(&self, file_id: FileId) -> Result<usize, DatabaseError> {
        if !self.file_exists(file_id)? {
            return Err(DatabaseError::MissingFile);
        }

        shortest_unique_prefix_length(&self.connection, "files_v2", "id", &file_id.to_string())
    }

    /// Resolve a full-or-short file id `prefix` to a single [`FileId`].
    ///
    /// The inverse of [`shorten_file_id`](Self::shorten_file_id): given the
    /// characters a user typed (a short id from a listing, or a full id pasted
    /// in either hyphenated or hex form), find the one file it identifies.
    ///
    /// Errors:
    /// - [`DatabaseError::MissingFile`] if no file matches the prefix.
    /// - [`DatabaseError::AmbiguousIdPrefix`] if more than one file matches
    ///   (e.g. a colliding file was added since the short id was displayed).
    pub fn resolve_file_id_prefix(&self, prefix: &str) -> Result<FileId, DatabaseError> {
        let normalized = normalize_id_prefix(prefix).ok_or(DatabaseError::MissingFile)?;
        match resolve_id_prefix(&self.connection, "files_v2", "id", &normalized)? {
            PrefixResolution::Unique(id) => {
                FileId::from_string(&id).ok_or(DatabaseError::MissingFile)
            }
            PrefixResolution::NotFound => Err(DatabaseError::MissingFile),
            PrefixResolution::Ambiguous => Err(DatabaseError::AmbiguousIdPrefix(normalized)),
        }
    }

    /// Compute the shortest unique prefix of `tag_id` among **all** tags — the
    /// "short id" shown in listings. The tag counterpart of
    /// [`shorten_file_id`](Self::shorten_file_id); see it for the
    /// neighbour-based reasoning and the caveats about the length not being
    /// stable across concurrent inserts.
    ///
    /// Returns `MissingTag` if `tag_id` is not in `tags`.
    pub fn shorten_tag_id(&self, tag_id: TagId) -> Result<usize, DatabaseError> {
        if !self.tag_exists(tag_id)? {
            return Err(DatabaseError::MissingTag);
        }

        shortest_unique_prefix_length(&self.connection, "tags_v1", "id", &tag_id.to_string())
    }

    /// Resolve a full-or-short tag id `prefix` to a single [`TagId`]. The tag
    /// counterpart of [`resolve_file_id_prefix`](Self::resolve_file_id_prefix).
    ///
    /// Errors:
    /// - [`DatabaseError::MissingTag`] if no tag matches the prefix.
    /// - [`DatabaseError::AmbiguousIdPrefix`] if more than one tag matches.
    pub fn resolve_tag_id_prefix(&self, prefix: &str) -> Result<TagId, DatabaseError> {
        let normalized = normalize_id_prefix(prefix).ok_or(DatabaseError::MissingTag)?;
        match resolve_id_prefix(&self.connection, "tags_v1", "id", &normalized)? {
            PrefixResolution::Unique(id) => {
                TagId::from_string(&id).ok_or(DatabaseError::MissingTag)
            }
            PrefixResolution::NotFound => Err(DatabaseError::MissingTag),
            PrefixResolution::Ambiguous => Err(DatabaseError::AmbiguousIdPrefix(normalized)),
        }
    }
}

#[cfg(test)]
mod tests {
    use tagsy_core::LogicalPath;

    use super::*;
    use crate::store::DeletedRule;
    use crate::store::fixtures::{file_id_from_hex, memory_db, tag_id_from_hex};

    #[test]
    fn shorten_file_id_single_file_needs_one_char() {
        let database = memory_db();
        let only = file_id_from_hex("00000000000000000000000000000001");
        database.add_file(only, &LogicalPath::new("a"), 0).unwrap();

        // No neighbours -> a single character already uniquely identifies it.
        assert_eq!(database.shorten_file_id(only).unwrap(), 1);
    }

    #[test]
    fn shorten_file_id_grows_prefix_to_disambiguate_neighbours() {
        let database = memory_db();
        // Three ids: two share the leading `abcd`, one is far away.
        let shared_a = file_id_from_hex("abcd000000000000000000000000000a");
        let shared_b = file_id_from_hex("abcd000000000000000000000000000b");
        let far = file_id_from_hex("ffff000000000000000000000000000f");
        for (id, name) in [(shared_a, "a"), (shared_b, "b"), (far, "c")] {
            database.add_file(id, &LogicalPath::new(name), 0).unwrap();
        }

        // shared_a and shared_b agree on `abcd00...000` up to the final hex
        // char, so they must be distinguished at the last differing position.
        let len_a = database.shorten_file_id(shared_a).unwrap();
        let len_b = database.shorten_file_id(shared_b).unwrap();
        let a = shared_a.to_string();
        let b = shared_b.to_string();
        // The prefix of each must exclude the other.
        assert!(!b.starts_with(&a[..len_a]));
        assert!(!a.starts_with(&b[..len_b]));

        // The far id only needs one char (`f`), since neither neighbour shares
        // its first character.
        assert_eq!(database.shorten_file_id(far).unwrap(), 1);
    }

    #[test]
    fn shorten_file_id_missing_is_not_found() {
        let database = memory_db();
        assert!(matches!(
            database.shorten_file_id(FileId::new()),
            Err(DatabaseError::MissingFile)
        ));
    }

    #[test]
    fn get_all_files_reports_short_id_length() {
        let mut database = memory_db();
        let shared_a = file_id_from_hex("abcd000000000000000000000000000a");
        let shared_b = file_id_from_hex("abcd000000000000000000000000000b");
        for (id, name) in [(shared_a, "a"), (shared_b, "b")] {
            database.add_file(id, &LogicalPath::new(name), 0).unwrap();
            database.record_version(id, "hash", "local", 1).unwrap();
        }

        let files = database.get_all_files(DeletedRule::Exclude).unwrap();
        // Both ids share all but the final character, so the short id must be
        // the full length to disambiguate.
        for info in &files {
            let full = info.file_id.to_string();
            assert_eq!(info.short_id_length, full.len());
        }
    }

    #[test]
    fn resolve_file_id_prefix_unique_short_prefix() {
        let database = memory_db();
        let far_a = file_id_from_hex("aaaa000000000000000000000000000a");
        let far_b = file_id_from_hex("bbbb000000000000000000000000000b");
        database.add_file(far_a, &LogicalPath::new("a"), 0).unwrap();
        database.add_file(far_b, &LogicalPath::new("b"), 0).unwrap();

        // A single leading char is enough to pick each out.
        assert_eq!(database.resolve_file_id_prefix("a").unwrap(), far_a);
        assert_eq!(database.resolve_file_id_prefix("b").unwrap(), far_b);
    }

    #[test]
    fn resolve_file_id_prefix_accepts_full_and_hyphenated_forms() {
        let database = memory_db();
        let id = file_id_from_hex("7f3a1b2c4d5e6f708192a3b4c5d6e7f8");
        database.add_file(id, &LogicalPath::new("a"), 0).unwrap();

        // Full hex form.
        assert_eq!(
            database
                .resolve_file_id_prefix("7f3a1b2c4d5e6f708192a3b4c5d6e7f8")
                .unwrap(),
            id
        );
        // Hyphenated form (hyphens are stripped before matching).
        assert_eq!(
            database
                .resolve_file_id_prefix("7f3a1b2c-4d5e-6f70-8192-a3b4c5d6e7f8")
                .unwrap(),
            id
        );
    }

    #[test]
    fn resolve_file_id_prefix_ambiguous_is_reported() {
        let database = memory_db();
        let shared_a = file_id_from_hex("abcd000000000000000000000000000a");
        let shared_b = file_id_from_hex("abcd000000000000000000000000000b");
        database
            .add_file(shared_a, &LogicalPath::new("a"), 0)
            .unwrap();
        database
            .add_file(shared_b, &LogicalPath::new("b"), 0)
            .unwrap();

        // `abcd` matches both.
        assert!(matches!(
            database.resolve_file_id_prefix("abcd"),
            Err(DatabaseError::AmbiguousIdPrefix(prefix)) if prefix == "abcd"
        ));
    }

    #[test]
    fn resolve_file_id_prefix_unknown_is_missing() {
        let database = memory_db();
        database
            .add_file(
                file_id_from_hex("aaaa000000000000000000000000000a"),
                &LogicalPath::new("a"),
                0,
            )
            .unwrap();

        assert!(matches!(
            database.resolve_file_id_prefix("ffff"),
            Err(DatabaseError::MissingFile)
        ));
    }

    #[test]
    fn resolve_file_id_prefix_rejects_non_hex() {
        let database = memory_db();
        // `zzzz` is not hex; normalization fails and it resolves to nothing.
        assert!(matches!(
            database.resolve_file_id_prefix("zzzz"),
            Err(DatabaseError::MissingFile)
        ));
    }

    #[test]
    fn shorten_then_resolve_roundtrips() {
        let mut database = memory_db();
        let shared_a = file_id_from_hex("abcd000000000000000000000000000a");
        let shared_b = file_id_from_hex("abcd000000000000000000000000000b");
        let far = file_id_from_hex("ffff000000000000000000000000000f");
        for (id, name) in [(shared_a, "a"), (shared_b, "b"), (far, "c")] {
            database.add_file(id, &LogicalPath::new(name), 0).unwrap();
            database.record_version(id, "hash", "local", 1).unwrap();
        }

        // Each file's displayed short id must resolve back to exactly itself.
        for info in database.get_all_files(DeletedRule::Exclude).unwrap() {
            let full = info.file_id.to_string();
            let short = &full[..info.short_id_length];
            assert_eq!(
                database.resolve_file_id_prefix(short).unwrap(),
                info.file_id,
                "short id {short} should resolve to its own file"
            );
        }
    }

    #[test]
    fn resolve_tag_id_prefix_unique_short_prefix() {
        let database = memory_db();
        let far_a = tag_id_from_hex("aaaa000000000000000000000000000a");
        let far_b = tag_id_from_hex("bbbb000000000000000000000000000b");
        database.add_tag(far_a, "a", "red", 1).unwrap();
        database.add_tag(far_b, "b", "red", 1).unwrap();

        assert_eq!(database.resolve_tag_id_prefix("a").unwrap(), far_a);
        assert_eq!(database.resolve_tag_id_prefix("b").unwrap(), far_b);
    }

    #[test]
    fn resolve_tag_id_prefix_ambiguous_is_reported() {
        let database = memory_db();
        let shared_a = tag_id_from_hex("abcd000000000000000000000000000a");
        let shared_b = tag_id_from_hex("abcd000000000000000000000000000b");
        database.add_tag(shared_a, "a", "red", 1).unwrap();
        database.add_tag(shared_b, "b", "red", 1).unwrap();

        assert!(matches!(
            database.resolve_tag_id_prefix("abcd"),
            Err(DatabaseError::AmbiguousIdPrefix(prefix)) if prefix == "abcd"
        ));
    }

    #[test]
    fn resolve_tag_id_prefix_unknown_is_missing() {
        let database = memory_db();
        database
            .add_tag(
                tag_id_from_hex("aaaa000000000000000000000000000a"),
                "a",
                "red",
                1,
            )
            .unwrap();

        assert!(matches!(
            database.resolve_tag_id_prefix("ffff"),
            Err(DatabaseError::MissingTag)
        ));
    }

    #[test]
    fn shorten_then_resolve_tag_roundtrips() {
        let database = memory_db();
        let shared_a = tag_id_from_hex("abcd000000000000000000000000000a");
        let shared_b = tag_id_from_hex("abcd000000000000000000000000000b");
        let far = tag_id_from_hex("ffff000000000000000000000000000f");
        for (id, name) in [(shared_a, "a"), (shared_b, "b"), (far, "c")] {
            database.add_tag(id, name, "red", 1).unwrap();
        }

        // Each tag's displayed short id must resolve back to exactly itself.
        for tag in database.get_all_tags(DeletedRule::Exclude).unwrap() {
            let full = tag.id.to_string();
            let length = database.shorten_tag_id(tag.id).unwrap();
            let short = &full[..length];
            assert_eq!(
                database.resolve_tag_id_prefix(short).unwrap(),
                tag.id,
                "short id {short} should resolve to its own tag"
            );
        }
    }
}
