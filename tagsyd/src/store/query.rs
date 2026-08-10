//! Search: the query vocabulary (`QueryTerm` / `TextPattern`) and the two
//! functions that evaluate it.
//!
//! Pure composition over the other modules' primitives — this module issues no
//! SQL of its own.

use std::collections::BTreeSet;

use tagsy_core::{FileId, TagId};
use regex::{Regex, RegexBuilder};

use super::CatalogStore;
use super::types::{DatabaseError, DeletedRule, SubtagRule};

/// A single clause of a file search (see [`CatalogStore::file_ids_for_query`]).
///
/// Terms are combined conjunctively: a file matches a query only if it matches
/// *every* term. Each term corresponds to one parsed chunk from the API layer
/// (`api::chunk::Chunk`) after its tag references have been resolved to
/// [`TagId`] sets.
///
/// A single tag token can match a *set* of tags (every tag whose name contains
/// the token substring or whose id starts with it), so the tag-bearing terms
/// carry a resolved [`TagId`] set rather than a single id; the set is expanded
/// from the user's syntax in a higher layer (the daemon's API), keeping this
/// database primitive free of name/prefix-resolution concerns. An empty set
/// matches no tag (so `HasTag([])` matches no file and `NotTag([])` excludes
/// nothing).
///
/// The `Any`/`NotAny` variants correspond to a *prefix-less* chunk: the user
/// wrote `foo`, and it should match anything that could reasonably relate to
/// `foo` — its logical path contains `foo`, *or* it carries/subtags a tag
/// resolved from `foo`. These variants carry both the raw substring and the
/// resolved tag set so both sides of the disjunction can be evaluated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryTerm {
    /// Must carry *at least one* tag in this set.
    HasTag(Vec<TagId>),
    /// Must carry *none* of the tags in this set.
    NotTag(Vec<TagId>),
    /// The logical path of the file or name of the tag must match the pattern.
    NameMatches(TextPattern),
    /// The logical path of the file or name of the tag must *not* match the
    /// pattern.
    NotNameMatches(TextPattern),
    /// The logical path of the file must match the pattern.
    LogicalMatches(TextPattern),
    /// The logical path of the file must *not* match the pattern.
    NotLogicalMatches(TextPattern),
    /// Matches on *either* text OR tag (union across both axes). The
    /// [`TextPattern`] is the text side; the [`Vec<TagId>`] is the resolved tag
    /// set for the same token. An empty tag set here does **not** mean "matches
    /// nothing" — the text side still stands.
    AnyMatch(TextPattern, Vec<TagId>),
    /// Negation of [`QueryTerm::AnyMatch`]: must match *neither* the text nor
    /// any tag in the set.
    NotAnyMatch(TextPattern, Vec<TagId>),
}

/// How the text half of a [`QueryTerm`] should be interpreted.
///
/// Which one a chunk produces is decided purely by how the user delimited it
/// (see the `chunk` module in `api.rs`): a bare or `"`-quoted payload is a
/// [`Substring`](Self::Substring), a `%`-delimited one is a
/// [`Regex`](Self::Regex).
///
/// The pattern is carried as a [`String`], not a compiled `Regex`, so that
/// `QueryTerm` stays `Clone + PartialEq + Eq` (a compiled regex is none of
/// those) and so the parsed query remains a plain value that is cheap to
/// inspect and compare in tests. Compilation happens once per query inside
/// [`CatalogStore::file_ids_for_query`] / [`CatalogStore::tag_ids_for_query`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TextPattern {
    /// A literal substring, matched case-insensitively.
    Substring(String),
    /// A regular expression, matched case-insensitively by default. The
    /// default is inverted rather than fixed: an author who wants case to
    /// matter writes `(?-i)` at the front of the pattern.
    ///
    /// Case-insensitive is the default because everything else in search is,
    /// and a `%Cat%` that quietly behaved differently from `Cat` would be
    /// impossible to predict.
    Regex(String),
}

impl TextPattern {
    /// Prepare this pattern for repeated matching.
    ///
    /// Done once per query rather than per candidate: compiling a regex costs
    /// far more than running one, and search is re-issued on every keystroke
    /// (the UI debounces at 200 ms).
    pub(super) fn compile(&self) -> CompiledPattern {
        match self {
            TextPattern::Substring(needle) => CompiledPattern::Substring(needle.to_lowercase()),
            TextPattern::Regex(pattern) => {
                match RegexBuilder::new(pattern).case_insensitive(true).build() {
                    Ok(regex) => CompiledPattern::Regex(regex),
                    // A pattern that does not compile matches nothing.
                    //
                    // Silently dropping the *term* instead would widen the
                    // result set, so a half-typed `%foo(%` would flash the
                    // user's entire library on screen. Matching nothing keeps
                    // a broken term restrictive, which is both safer and what
                    // an empty result set already communicates. Unlike the
                    // lexer's other recoveries this is not invisible: the user
                    // typed `%`, so they know they asked for a regex.
                    Err(error) => {
                        log::debug!("query regex {pattern:?} did not compile: {error}");
                        CompiledPattern::Never
                    }
                }
            }
        }
    }
}

/// A [`TextPattern`] prepared for matching. See [`TextPattern::compile`].
pub(super) enum CompiledPattern {
    /// Pre-lowercased needle, tested against the pre-lowercased haystack.
    Substring(String),
    Regex(Regex),
    /// A regex that failed to compile.
    Never,
}

impl CompiledPattern {
    /// Test both spellings of one haystack.
    ///
    /// Callers already hold a lowercased copy of every candidate's text (built
    /// once per query), so substring matching can use it directly instead of
    /// allocating per comparison. Regex matching deliberately uses the
    /// *original* text: the case-insensitivity is baked into the compiled
    /// regex, so feeding it a lowercased haystack would silently defeat a
    /// `(?-i)` opt-out.
    pub(super) fn is_match(&self, original: &str, lowercased: &str) -> bool {
        match self {
            CompiledPattern::Substring(needle) => lowercased.contains(needle.as_str()),
            CompiledPattern::Regex(regex) => regex.is_match(original),
            CompiledPattern::Never => false,
        }
    }
}

impl CatalogStore {
    /// Search files by a conjunction of [`QueryTerm`]s.
    ///
    /// A file is returned only if it satisfies every term:
    /// - [`QueryTerm::HasTag`]: it carries *at least one* tag in the set
    ///   (subtag traversal governed by `subtag_rule`).
    /// - [`QueryTerm::NotTag`]: it carries *none* of the tags in the set (same
    ///   traversal).
    /// - [`QueryTerm::LogicalMatches`] / [`QueryTerm::NotLogicalMatches`]: its
    ///   logical path matches / does not match the [`TextPattern`],
    ///   case-insensitively either way.
    /// - [`QueryTerm::AnyMatch`] / [`QueryTerm::NotAnyMatch`]: its logical path
    ///   matches the pattern **or** it carries any tag in the set — the
    ///   "prefix-less" chunk semantics.
    ///
    /// An empty term list matches every file; an empty tag set inside a term
    /// matches no tag (so `HasTag([])` matches nothing and `NotTag([])`
    /// excludes nothing). For [`QueryTerm::AnyMatch`] the text side
    /// still stands when the tag set is empty. Composes
    /// [`Self::file_ids_for_tag`] and [`Self::get_all_files`]; no new SQL.
    ///
    /// `deleted_rule` is passed through to [`Self::get_all_files`] so that
    /// tombstoned files can participate in the candidate pool under
    /// [`DeletedRule::Include`]. Relationship traversal (`entries_v1`)
    /// stays live-only regardless — only the file's own row visibility is
    /// affected. Callers filter the returned ids by their `deleted` flag if
    /// they want an only-deleted view.
    pub fn file_ids_for_query(
        &self,
        terms: &[QueryTerm],
        subtag_rule: SubtagRule,
        deleted_rule: DeletedRule,
    ) -> Result<impl IntoIterator<Item = FileId>, DatabaseError> {
        // The set of files carrying at least one tag in `tag_ids` (the union of
        // each tag's files). An empty set yields no files.
        let files_for_any_tag = |tag_ids: &[TagId]| -> Result<BTreeSet<FileId>, DatabaseError> {
            let mut union = BTreeSet::new();
            for tag_id in tag_ids {
                union.extend(self.file_ids_for_tag(*tag_id, subtag_rule)?);
            }
            Ok(union)
        };

        // Seed the candidate set. If there is at least one positive tag-only
        // term (`HasTag`), start from the intersection of those (each
        // contributing the union of its matched tags' files); otherwise start
        // from all files. `AnyMatch` is *not* a seed — its substring half can
        // match files that carry no tags at all, so we always need every file
        // in the candidate pool for the filter pass below.
        let positive: Vec<&[TagId]> = terms
            .iter()
            .filter_map(|term| match term {
                QueryTerm::HasTag(tag_ids) => Some(tag_ids.as_slice()),
                _ => None,
            })
            .collect();

        let mut candidates: BTreeSet<FileId> = if let Some((first, rest)) = positive.split_first() {
            let mut set = files_for_any_tag(first)?;
            for tag_ids in rest {
                let next = files_for_any_tag(tag_ids)?;
                set.retain(|file_id| next.contains(file_id));
            }
            // A `HasTag` seed comes from `entries_v1` (always live) and does
            // not consult `files_v2.deleted`, so it can include ids for
            // tombstoned files. We do NOT drop them here — `Api::run_query`
            // joins each id through `file_info_from_id` (which honors the
            // requested `deleted_rule`) and skips misses. This preserves the
            // long-standing tolerance for files referenced from `entries_v1`
            // without a matching `file_versions` row yet.
            set
        } else {
            self.get_all_files(deleted_rule)?
                .into_iter()
                .map(|file| file.file_id)
                .collect()
        };

        // Subtract every negative tag term (the union of its matched tags' files).
        for term in terms {
            if let QueryTerm::NotTag(tag_ids) = term {
                let excluded = files_for_any_tag(tag_ids)?;
                candidates.retain(|file_id| !excluded.contains(file_id));
            }
        }

        // Text-bearing terms (`LogicalContains`, `NotLogicalContains`,
        // `AnyMatch`, `NotAnyMatch`) need each candidate's logical path *and*,
        // for the `Any` variants, the union of files-carrying-any-of-the-set.
        // Build both lookups once, then apply every text term as one retain
        // pass — cheaper than rebuilding per term and keeps the semantics
        // obvious.
        let has_text_term = terms.iter().any(|term| {
            matches!(
                term,
                QueryTerm::NameMatches(_)
                    | QueryTerm::NotNameMatches(_)
                    | QueryTerm::LogicalMatches(_)
                    | QueryTerm::NotLogicalMatches(_)
                    | QueryTerm::AnyMatch(_, _)
                    | QueryTerm::NotAnyMatch(_, _),
            )
        });

        if has_text_term {
            // Both spellings of each path: the original for regex matching and
            // a lowercased copy for substring matching. See
            // `CompiledPattern::is_match` for why the regex side must not see
            // the lowercased form.
            let paths: std::collections::BTreeMap<FileId, (String, String)> = self
                .get_all_files(deleted_rule)?
                .into_iter()
                .map(|file| {
                    let path = file.logical_path.as_str().to_owned();
                    let lowercased = path.to_lowercase();
                    (file.file_id, (path, lowercased))
                })
                .collect();

            // Compile every pattern, and precompute the file set for every
            // `Any` variant, before entering the retain closure: neither a
            // regex build nor a DB hit belongs inside a per-candidate loop.
            let mut patterns: Vec<(CompiledPattern, bool)> = Vec::new();
            let mut any_tag_sets: Vec<(CompiledPattern, BTreeSet<FileId>, bool)> = Vec::new();
            for term in terms {
                match term {
                    QueryTerm::NameMatches(pattern) | QueryTerm::LogicalMatches(pattern) => {
                        patterns.push((pattern.compile(), false));
                    }
                    QueryTerm::NotNameMatches(pattern) | QueryTerm::NotLogicalMatches(pattern) => {
                        patterns.push((pattern.compile(), true));
                    }
                    QueryTerm::AnyMatch(pattern, tag_ids) => {
                        any_tag_sets.push((pattern.compile(), files_for_any_tag(tag_ids)?, false));
                    }
                    QueryTerm::NotAnyMatch(pattern, tag_ids) => {
                        any_tag_sets.push((pattern.compile(), files_for_any_tag(tag_ids)?, true));
                    }
                    _ => continue,
                }
            }

            candidates.retain(|file_id| {
                let Some((path, lowercased)) = paths.get(file_id) else {
                    return false;
                };
                for (pattern, negated) in &patterns {
                    if pattern.is_match(path, lowercased) == *negated {
                        return false;
                    }
                }
                for (pattern, tagged_files, negated) in &any_tag_sets {
                    let hit = pattern.is_match(path, lowercased) || tagged_files.contains(file_id);
                    if hit == *negated {
                        return false;
                    }
                }
                true
            });
        }

        Ok(candidates)
    }

    /// Search *tags* by the same conjunction of [`QueryTerm`]s used for files,
    /// mirroring [`Self::file_ids_for_query`] onto the tag hierarchy:
    ///
    /// - [`QueryTerm::HasTag`]: the tag must be a subtag of *at least one* tag
    ///   in the set (subtag traversal governed by `subtag_rule`) — the tag
    ///   analogue of "a file carries this tag".
    /// - [`QueryTerm::NotTag`]: the tag must *not* be a subtag of any tag in
    ///   the set.
    /// - [`QueryTerm::NameMatches`] / [`QueryTerm::NotNameMatches`]: the tag's
    ///   name matches / does not match the [`TextPattern`], compared
    ///   case-insensitively.
    /// - [`QueryTerm::LogicalMatches`] / [`QueryTerm::NotLogicalMatches`]:
    ///   doesn't match.
    /// - [`QueryTerm::AnyMatch`] / [`QueryTerm::NotAnyMatch`]: the tag's name
    ///   matches the pattern **or** the tag is a subtag of any tag in the set —
    ///   the tag analogue of the file-side `Any` semantics.
    ///
    /// An empty term list matches every tag; an empty tag set inside a term
    /// matches no tag. For [`QueryTerm::AnyMatch`] the text side still
    /// stands when the tag set is empty. Composes [`Self::subtag_ids_for_tag`]
    /// and [`Self::get_all_tags`]; no new SQL.
    ///
    /// `deleted_rule` is passed through to [`Self::get_all_tags`] so that
    /// tombstoned tags can participate in the candidate pool under
    /// [`DeletedRule::Include`]. Relationship traversal (`entries_v1`)
    /// stays live-only regardless — only the tag's own row visibility is
    /// affected. Callers filter the returned ids by their `deleted` flag if
    /// they want an only-deleted view.
    pub fn tag_ids_for_query(
        &self,
        terms: &[QueryTerm],
        subtag_rule: SubtagRule,
        deleted_rule: DeletedRule,
    ) -> Result<impl IntoIterator<Item = TagId>, DatabaseError> {
        // The set of tags that are a subtag of at least one tag in `tag_ids`
        // (the union of each tag's subtags). An empty set yields no tags.
        let subtags_of_any = |tag_ids: &[TagId]| -> Result<BTreeSet<TagId>, DatabaseError> {
            let mut union = BTreeSet::new();
            for tag_id in tag_ids {
                union.extend(self.subtag_ids_for_tag(*tag_id, subtag_rule)?);
            }
            Ok(union)
        };

        // Seed on `HasTag` only, same reasoning as `file_ids_for_query`:
        // `AnyMatch` can match on the name side without any tag membership, so
        // we can't use it to narrow the seed.
        let positive: Vec<&[TagId]> = terms
            .iter()
            .filter_map(|term| match term {
                QueryTerm::HasTag(tag_ids) => Some(tag_ids.as_slice()),
                _ => None,
            })
            .collect();

        let mut candidates: BTreeSet<TagId> = if let Some((first, rest)) = positive.split_first() {
            let mut set = subtags_of_any(first)?;
            for tag_ids in rest {
                let next = subtags_of_any(tag_ids)?;
                set.retain(|tag_id| next.contains(tag_id));
            }
            // Subtag traversal comes from `entries_v1` (always live) and does
            // not consult `tags_v1.deleted`, so it can include ids for
            // tombstoned tags. We do NOT drop them here — `Api::run_query`
            // joins each id through `tag_from_id` (which honors the requested
            // `deleted_rule`) and skips misses, matching the tolerance in
            // `file_ids_for_query`.
            set
        } else {
            self.get_all_tags(deleted_rule)?
                .into_iter()
                .map(|tag| tag.id)
                .collect()
        };

        for term in terms {
            match term {
                QueryTerm::LogicalMatches(..) | QueryTerm::NotLogicalMatches(..) => {
                    candidates.clear();
                    return Ok(candidates);
                }
                QueryTerm::NotTag(tag_ids) => {
                    let excluded = subtags_of_any(tag_ids)?;
                    candidates.retain(|tag_id| !excluded.contains(tag_id));
                }
                _ => {}
            }
        }

        let has_text_term = terms.iter().any(|term| {
            matches!(
                term,
                QueryTerm::NameMatches(_)
                    | QueryTerm::NotNameMatches(_)
                    | QueryTerm::AnyMatch(_, _)
                    | QueryTerm::NotAnyMatch(_, _),
            )
        });

        if has_text_term {
            // Original + lowercased, for the same reason as in
            // `file_ids_for_query`.
            let names: std::collections::BTreeMap<TagId, (String, String)> = self
                .get_all_tags(deleted_rule)?
                .into_iter()
                .map(|tag| {
                    let lowercased = tag.name.to_lowercase();
                    (tag.id, (tag.name, lowercased))
                })
                .collect();

            let mut patterns: Vec<(CompiledPattern, bool)> = Vec::new();
            let mut any_tag_sets: Vec<(CompiledPattern, BTreeSet<TagId>, bool)> = Vec::new();
            for term in terms {
                match term {
                    QueryTerm::NameMatches(pattern) => patterns.push((pattern.compile(), false)),
                    QueryTerm::NotNameMatches(pattern) => {
                        patterns.push((pattern.compile(), true));
                    }
                    QueryTerm::AnyMatch(pattern, tag_ids) => {
                        any_tag_sets.push((pattern.compile(), subtags_of_any(tag_ids)?, false));
                    }
                    QueryTerm::NotAnyMatch(pattern, tag_ids) => {
                        any_tag_sets.push((pattern.compile(), subtags_of_any(tag_ids)?, true));
                    }
                    _ => continue,
                }
            }

            candidates.retain(|tag_id| {
                let Some((name, lowercased)) = names.get(tag_id) else {
                    return false;
                };
                for (pattern, negated) in &patterns {
                    if pattern.is_match(name, lowercased) == *negated {
                        return false;
                    }
                }
                for (pattern, subtagged, negated) in &any_tag_sets {
                    let hit = pattern.is_match(name, lowercased) || subtagged.contains(tag_id);
                    if hit == *negated {
                        return false;
                    }
                }
                true
            });
        }

        Ok(candidates)
    }
}

#[cfg(test)]
mod tests {
    use tagsy_core::LogicalPath;

    use super::*;
    use crate::clock::now_millis;
    use crate::store::fixtures::memory_db;

    /// Build a catalog of files at the given logical paths, returning the ids
    /// in the same order.
    fn files_at(database: &mut CatalogStore, paths: &[&str]) -> Vec<FileId> {
        paths
            .iter()
            .map(|path| {
                let file_id = FileId::new();
                database
                    .add_file(file_id, &LogicalPath::new(*path), 0)
                    .unwrap();
                database
                    .record_version(file_id, "hash", "local", 1)
                    .unwrap();
                file_id
            })
            .collect()
    }

    fn matching_files(database: &CatalogStore, terms: &[QueryTerm]) -> BTreeSet<FileId> {
        database
            .file_ids_for_query(terms, SubtagRule::Exclude, DeletedRule::Exclude)
            .unwrap()
            .into_iter()
            .collect()
    }

    fn regex(pattern: &str) -> TextPattern {
        TextPattern::Regex(pattern.to_owned())
    }

    #[test]
    fn regex_term_matches_against_the_full_logical_path() {
        let mut database = memory_db();
        let files = files_at(&mut database, &[
            "photos/holiday/cat.jpg",
            "archive/photos/cat.jpg",
            "notes/todo.md",
        ]);

        let terms = vec![QueryTerm::LogicalMatches(regex("^photos/"))];
        assert_eq!(
            matching_files(&database, &terms),
            BTreeSet::from([files[0]]),
            "anchoring must bind to the whole path, not the basename"
        );
    }

    /// Slashes in a pattern need no escaping — the reason the delimiter is `%`
    /// rather than the conventional `/`.
    #[test]
    fn regex_term_may_contain_slashes() {
        let mut database = memory_db();
        let files = files_at(&mut database, &[
            "photos/2024/raw/a.dng",
            "photos/2024/a.jpg",
        ]);

        let terms = vec![QueryTerm::LogicalMatches(regex(r"^photos/.*/raw/.*\.dng$"))];
        assert_eq!(
            matching_files(&database, &terms),
            BTreeSet::from([files[0]])
        );
    }

    /// Regexes are case-insensitive by default, matching every other text term.
    #[test]
    fn regex_term_is_case_insensitive_by_default() {
        let mut database = memory_db();
        let files = files_at(&mut database, &["Photos/Cat.JPG"]);

        let terms = vec![QueryTerm::LogicalMatches(regex(r"^photos/cat\.jpg$"))];
        assert_eq!(
            matching_files(&database, &terms),
            BTreeSet::from([files[0]])
        );
    }

    /// ...and `(?-i)` opts back out. This is the case that breaks if the
    /// matcher is handed a pre-lowercased haystack.
    #[test]
    fn regex_term_case_sensitivity_can_be_opted_into() {
        let mut database = memory_db();
        let files = files_at(&mut database, &["Photos/Cat.JPG", "photos/cat.jpg"]);

        let terms = vec![QueryTerm::LogicalMatches(regex(r"(?-i)^Photos/"))];
        assert_eq!(
            matching_files(&database, &terms),
            BTreeSet::from([files[0]]),
            "(?-i) must see the original casing"
        );
    }

    /// A pattern that does not compile matches nothing, rather than being
    /// dropped (which would *widen* the result set).
    #[test]
    fn invalid_regex_term_matches_nothing() {
        let mut database = memory_db();
        files_at(&mut database, &["notes/todo.md", "notes/todo.txt"]);

        let terms = vec![QueryTerm::LogicalMatches(regex("*.md"))];
        assert!(matching_files(&database, &terms).is_empty());
    }

    #[test]
    fn negated_regex_term_excludes_matches() {
        let mut database = memory_db();
        let files = files_at(&mut database, &["notes/todo.md", "notes/todo.txt"]);

        let terms = vec![QueryTerm::NotLogicalMatches(regex(r"\.md$"))];
        assert_eq!(
            matching_files(&database, &terms),
            BTreeSet::from([files[1]])
        );
    }

    /// A substring term keeps meaning exactly what it did: regex metacharacters
    /// in it are literal.
    #[test]
    fn substring_term_treats_metacharacters_literally() {
        let mut database = memory_db();
        let files = files_at(&mut database, &["notes/todo.md", "notes/todoXmd"]);

        let terms = vec![QueryTerm::LogicalMatches(TextPattern::Substring(
            "todo.md".to_owned(),
        ))];
        assert_eq!(
            matching_files(&database, &terms),
            BTreeSet::from([files[0]]),
            "`.` must not behave as a wildcard in a substring term"
        );
    }

    /// `/t %...%` resolves tags by regex over their names.
    #[test]
    fn regex_token_resolves_tags_by_name() {
        let database = memory_db();
        let wip = TagId::new();
        let done = TagId::new();
        database.add_tag(wip, "wip-draft", "red", 1).unwrap();
        database.add_tag(done, "done", "red", 1).unwrap();

        let matched = database
            .tag_ids_matching_pattern(&regex("^wip-"), DeletedRule::Exclude)
            .unwrap();
        assert_eq!(matched, vec![wip]);
    }

    /// Tag *ids* stay out of regex resolution: a pattern that would match a
    /// hex id prefix must not sweep tags in by id.
    #[test]
    fn regex_token_does_not_resolve_tags_by_id() {
        let database = memory_db();
        let tag_id = TagId::new();
        database.add_tag(tag_id, "unrelated", "red", 1).unwrap();

        // A pattern matching the tag's own id prefix, which the substring path
        // *would* resolve.
        let id = tag_id.to_string();
        let matched = database
            .tag_ids_matching_pattern(&regex(&format!("^{}", &id[..6])), DeletedRule::Exclude)
            .unwrap();
        assert!(matched.is_empty(), "ids are not a regex surface");

        // The substring path still resolves that same prefix, proving the test
        // is comparing the two paths rather than a typo.
        let matched = database
            .tag_ids_matching_pattern(
                &TextPattern::Substring(id[..6].to_owned()),
                DeletedRule::Exclude,
            )
            .unwrap();
        assert_eq!(matched, vec![tag_id]);
    }

    #[test]
    fn file_ids_for_query_positive_term_unions_all_matching_tags() {
        let database = memory_db();
        // Two distinct tags both containing the substring 'foo'.
        let foo = TagId::new();
        let foobar = TagId::new();
        database.add_tag(foo, "foo", "red", 1).unwrap();
        database.add_tag(foobar, "foobar", "red", 1).unwrap();

        // file_a carries `foo`, file_b carries `foobar`, file_c carries neither.
        let file_a = FileId::new();
        let file_b = FileId::new();
        let file_c = FileId::new();
        for (id, path) in [(file_a, "a"), (file_b, "b"), (file_c, "c")] {
            database.add_file(id, &LogicalPath::new(path), 0).unwrap();
        }
        database.tag_file(foo, file_a, 1).unwrap();
        database.tag_file(foobar, file_b, 1).unwrap();

        // `$foo` should match files carrying either tag (union), not require both.
        let terms = vec![QueryTerm::HasTag(vec![foo, foobar])];
        let matched: BTreeSet<FileId> = database
            .file_ids_for_query(&terms, SubtagRule::Exclude, DeletedRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(matched, BTreeSet::from([file_a, file_b]));
    }

    #[test]
    fn file_ids_for_query_empty_positive_set_matches_nothing() {
        let database = memory_db();
        let file = FileId::new();
        database.add_file(file, &LogicalPath::new("a"), 0).unwrap();

        // A `$foo` term that matched no tag (empty set) matches no file.
        let terms = vec![QueryTerm::HasTag(vec![])];
        let matched: Vec<FileId> = database
            .file_ids_for_query(&terms, SubtagRule::Exclude, DeletedRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();

        assert!(matched.is_empty());
    }

    #[test]
    fn file_ids_for_query_negative_term_excludes_union_of_matching_tags() {
        let mut database = memory_db();
        let foo = TagId::new();
        let foobar = TagId::new();
        database.add_tag(foo, "foo", "red", 1).unwrap();
        database.add_tag(foobar, "foobar", "red", 1).unwrap();

        let file_a = FileId::new();
        let file_b = FileId::new();
        let file_c = FileId::new();
        for (id, path) in [(file_a, "a"), (file_b, "b"), (file_c, "c")] {
            database.add_file(id, &LogicalPath::new(path), 0).unwrap();
            // A negative-only query seeds from all files, which requires each to
            // have a recorded version to appear in the listing.
            database.record_version(id, "hash", "test", 1).unwrap();
        }
        database.tag_file(foo, file_a, 1).unwrap();
        database.tag_file(foobar, file_b, 1).unwrap();

        // `!foo` excludes files carrying either matching tag, leaving only file_c.
        let terms = vec![QueryTerm::NotTag(vec![foo, foobar])];
        let matched: BTreeSet<FileId> = database
            .file_ids_for_query(&terms, SubtagRule::Exclude, DeletedRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(matched, BTreeSet::from([file_c]));
    }

    #[test]
    fn file_ids_for_query_include_returns_tombstoned_candidates() {
        // A `HasTag` search under `DeletedRule::Include` must surface files
        // whose row is tombstoned in `files_v2` (they still have live
        // `entries_v1` rows pointing at them). The caller is expected to
        // post-filter by `FileInfo::deleted` to keep only the tombstoned
        // ones; the evaluator does not do that step itself.
        let mut database = memory_db();
        let tag_id = TagId::new();
        database.add_tag(tag_id, "photos", "red", 10).unwrap();

        let live = FileId::new();
        let dead = FileId::new();
        database
            .add_file(live, &LogicalPath::new("live.jpg"), 0)
            .unwrap();
        database
            .add_file(dead, &LogicalPath::new("dead.jpg"), 0)
            .unwrap();
        database.record_version(live, "h1", "local", 1).unwrap();
        database.record_version(dead, "h2", "local", 1).unwrap();
        database.tag_file(tag_id, live, 20).unwrap();
        database.tag_file(tag_id, dead, 20).unwrap();

        let deleted_at = now_millis() + 10_000;
        assert!(database.remove_file(dead, deleted_at).unwrap());

        let terms = vec![QueryTerm::HasTag(vec![tag_id])];

        // Exclude does not drop the tombstoned candidate at the evaluator
        // layer either; the "hide deleted" filtering happens downstream in
        // `Api::run_query` (via `file_info_from_id` returning MissingFile).
        // What we assert here is that both ids reach the caller and the
        // downstream filter has enough information to distinguish them.
        let candidates: BTreeSet<FileId> = database
            .file_ids_for_query(&terms, SubtagRule::Exclude, DeletedRule::Include)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(candidates, BTreeSet::from([live, dead]));

        // Joining under `Include` returns both, tagged with `deleted`.
        let live_info = database
            .file_info_from_id(live, DeletedRule::Include)
            .unwrap();
        let dead_info = database
            .file_info_from_id(dead, DeletedRule::Include)
            .unwrap();
        assert!(!live_info.deleted);
        assert!(dead_info.deleted);
    }
}
