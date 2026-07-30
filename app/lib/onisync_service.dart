// String-id convenience layer over the generated [onisync.OniSyncApp].
//
// The generated API is split: listings return DTOs with *string* ids
// (FileEntry.fileId, TagEntry.tagId), but the write methods take *opaque* id
// handles (FileId, TagId) that Dart can only obtain via resolveFileId /
// resolveTagId. This extension centralises the "resolve the string, then call
// the op" pattern so the UI can work purely in terms of the id strings it
// already has from the DTOs. (The query methods that *return* string ids —
// tagIdsForFileString / fileIdsForTagString — live on the generated API
// directly, since the bridge already returns strings there.)

import 'rust/api.dart' as onisync;

extension OniSyncServiceX on onisync.OniSyncApp {
  /// Delete a tag by its string id (as shown in TagEntry.tagId).
  Future<void> deleteTagByString(String tagId) async {
    await deleteTag(tagId: await resolveTagId(prefix: tagId));
  }

  /// Restore a soft-deleted tag by its string id.
  Future<void> restoreTagByString(String tagId) async {
    await restoreTag(tagId: await resolveTagId(prefix: tagId));
  }

  /// Rename a tag identified by its string id.
  Future<void> renameTagByString({
    required String tagId,
    required String name,
  }) async {
    await renameTag(tagId: await resolveTagId(prefix: tagId), name: name);
  }

  /// Change the color of a tag identified by its string id.
  Future<void> setTagColorByString({
    required String tagId,
    required String color,
  }) async {
    await setTagColor(tagId: await resolveTagId(prefix: tagId), color: color);
  }

  /// Delete a file by its string id (as shown in FileEntry.fileId).
  Future<void> deleteFileByString(String fileId) async {
    await deleteFile(fileId: await resolveFileId(prefix: fileId));
  }

  /// Restore a soft-deleted file by its string id. Best-effort: throws if no
  /// source still holds the file's bytes.
  Future<void> restoreFileByString(String fileId) async {
    await restoreFile(fileId: await resolveFileId(prefix: fileId));
  }

  /// Apply a tag (by string id) to a file (by string id).
  Future<void> tagFileByString({
    required String tagId,
    required String fileId,
  }) async {
    final tid = await resolveTagId(prefix: tagId);
    final fid = await resolveFileId(prefix: fileId);
    await tagFile(tagId: tid, fileId: fid);
  }

  /// Remove a tag (by string id) from a file (by string id).
  Future<void> untagFileByString({
    required String tagId,
    required String fileId,
  }) async {
    final tid = await resolveTagId(prefix: tagId);
    final fid = await resolveFileId(prefix: fileId);
    await untagFile(tagId: tid, fileId: fid);
  }
}
