using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using SongCore.Data;

namespace SongCore.Utilities
{
    internal static class BinaryCache
    {
        private const string Magic = "SC02";
        private const int FormatVersion = 2;

        internal static readonly string CachePath = Path.Combine(
            IPA.Utilities.UnityGame.UserDataPath, nameof(SongCore), "SongCoreCache.bin");


        internal class CacheEntry
        {
            public string RelativePath;
            public long DirTimestamp;
            public string SongHash;
            public float Duration;
            public string LevelId;
            public string InfoDatJson;
            public string SongDataJson;
        }


        private static ConcurrentDictionary<string, CacheEntry> _entries = new ConcurrentDictionary<string, CacheEntry>(StringComparer.OrdinalIgnoreCase);

        internal static int Count => _entries.Count;

        internal static void Load()
        {
            _entries.Clear();

            if (File.Exists(CachePath))
            {
                try
                {
                    LoadBinary();
                    Plugin.Log.Info($"Loaded binary cache: {_entries.Count} entries from {CachePath}");
                    return;
                }
                catch (Exception ex)
                {
                    Plugin.Log.Warn($"Failed to load binary cache, will rebuild: {ex.Message}");
                    _entries.Clear();
                }
            }


            LoadLegacyJsonCaches();
        }

        internal static bool TryGetValid(string relativePath, long currentDirTimestamp, out CacheEntry entry)
        {
            if (_entries.TryGetValue(relativePath, out entry) && entry.DirTimestamp == currentDirTimestamp)
            {
                return true;
            }

            entry = null;
            return false;
        }

        internal static bool TryGet(string relativePath, out CacheEntry entry)
        {
            return _entries.TryGetValue(relativePath, out entry);
        }

        internal static void Set(string relativePath, CacheEntry entry)
        {
            entry.RelativePath = relativePath;
            _entries[relativePath] = entry;
        }

        internal static bool Remove(string relativePath)
        {
            return _entries.TryRemove(relativePath, out _);
        }

        internal static void SaveAndPrune(ICollection<string> activePaths)
        {

            var activeSet = new HashSet<string>(activePaths, StringComparer.OrdinalIgnoreCase);
            foreach (var key in _entries.Keys)
            {
                var absolutePath = Hashing.GetAbsolutePath(key);
                if (!activeSet.Contains(absolutePath) && !activeSet.Contains(key))
                {
                    _entries.TryRemove(key, out _);
                }
            }

            try
            {
                SaveBinary();
                Plugin.Log.Info($"Saved binary cache: {_entries.Count} entries to {CachePath}");
            }
            catch (Exception ex)
            {
                Plugin.Log.Error($"Failed to save binary cache: {ex.Message}");
                Plugin.Log.Error(ex);
            }
        }

        internal static IEnumerable<KeyValuePair<string, CacheEntry>> GetAllEntries()
        {
            return _entries;
        }

        #region Binary Format I/O

        private static void LoadBinary()
        {
            using var fs = new FileStream(CachePath, FileMode.Open, FileAccess.Read, FileShare.Read, 65536);
            using var reader = new BinaryReader(fs, Encoding.UTF8, leaveOpen: false);


            var magic = reader.ReadString();
            if (magic != Magic)
            {
                throw new InvalidDataException($"Invalid cache magic: expected '{Magic}', got '{magic}'");
            }

            var version = reader.ReadInt32();
            if (version != FormatVersion)
            {
                throw new InvalidDataException($"Unsupported cache version: {version}");
            }

            var count = reader.ReadInt32();

            for (int i = 0; i < count; i++)
            {
                var entry = new CacheEntry
                {
                    RelativePath = reader.ReadString(),
                    DirTimestamp = reader.ReadInt64(),
                    SongHash = reader.ReadString(),
                    Duration = reader.ReadSingle(),
                    LevelId = reader.ReadString(),
                    InfoDatJson = reader.ReadString(),
                    SongDataJson = reader.ReadString()
                };

                _entries[entry.RelativePath] = entry;
            }
        }

        private static void SaveBinary()
        {
            var tempPath = CachePath + ".tmp";
            using (var fs = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, 65536))
            using (var writer = new BinaryWriter(fs, Encoding.UTF8, leaveOpen: false))
            {
                writer.Write(Magic);
                writer.Write(FormatVersion);
                writer.Write(_entries.Count);

                foreach (var kvp in _entries)
                {
                    var entry = kvp.Value;
                    writer.Write(entry.RelativePath ?? string.Empty);
                    writer.Write(entry.DirTimestamp);
                    writer.Write(entry.SongHash ?? string.Empty);
                    writer.Write(entry.Duration);
                    writer.Write(entry.LevelId ?? string.Empty);
                    writer.Write(entry.InfoDatJson ?? string.Empty);
                    writer.Write(entry.SongDataJson ?? string.Empty);
                }
            }

            if (File.Exists(CachePath))
            {
                File.Delete(CachePath);
            }
            File.Move(tempPath, CachePath);
        }

        #endregion

        #region Legacy JSON Migration

        private static void LoadLegacyJsonCaches()
        {
            int migrated = 0;


            if (File.Exists(Hashing.cachedHashDataPath))
            {
                try
                {
                    using var reader = new JsonTextReader(new StreamReader(Hashing.cachedHashDataPath));
                    var serializer = JsonSerializer.CreateDefault();
                    var hashData = serializer.Deserialize<ConcurrentDictionary<string, SongHashData>>(reader);
                    if (hashData != null)
                    {
                        foreach (var kvp in hashData)
                        {
                            var entry = GetOrCreate(kvp.Key);
                            entry.DirTimestamp = kvp.Value.directoryHash;
                            entry.SongHash = kvp.Value.songHash;
                            migrated++;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Plugin.Log.Warn($"Failed to migrate legacy hash cache: {ex.Message}");
                }
            }


            if (File.Exists(Hashing.cachedAudioDataPath))
            {
                try
                {
                    using var reader = new JsonTextReader(new StreamReader(Hashing.cachedAudioDataPath));
                    var serializer = JsonSerializer.CreateDefault();
                    var audioData = serializer.Deserialize<ConcurrentDictionary<string, AudioCacheData>>(reader);
                    if (audioData != null)
                    {
                        foreach (var kvp in audioData)
                        {
                            var entry = GetOrCreate(kvp.Key);
                            entry.Duration = kvp.Value.duration;
                            entry.LevelId = kvp.Value.id;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Plugin.Log.Warn($"Failed to migrate legacy duration cache: {ex.Message}");
                }
            }

            if (migrated > 0)
            {
                Plugin.Log.Info($"Migrated {migrated} entries from legacy JSON caches to binary format.");
            }
        }

        private static CacheEntry GetOrCreate(string relativePath)
        {
            return _entries.GetOrAdd(relativePath, _ => new CacheEntry { RelativePath = relativePath });
        }

        #endregion

        #region Backward Compatibility Helpers

        internal static void PopulateLegacyHashDictionary(ConcurrentDictionary<string, SongHashData> target)
        {
            target.Clear();
            foreach (var kvp in _entries)
            {
                if (!string.IsNullOrEmpty(kvp.Value.SongHash))
                {
                    target[kvp.Key] = new SongHashData(kvp.Value.DirTimestamp, kvp.Value.SongHash);
                }
            }
        }

        internal static void PopulateLegacyAudioDictionary(ConcurrentDictionary<string, AudioCacheData> target)
        {
            target.Clear();
            foreach (var kvp in _entries)
            {
                if (kvp.Value.Duration > 0)
                {
                    target[kvp.Key] = new AudioCacheData(kvp.Value.LevelId ?? string.Empty, kvp.Value.Duration);
                }
            }
        }

        #endregion
    }
}
