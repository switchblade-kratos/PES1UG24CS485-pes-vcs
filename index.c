// index.c — Staging area implementation
//
// Text format of .pes/index (one entry per line, sorted by path):
//
//   <mode-octal> <64-char-hex-hash> <mtime-seconds> <size> <path>
//
// Example:
//   100644 a1b2c3d4e5f6...  1699900000 42 README.md
//   100644 f7e8d9c0b1a2...  1699900100 128 src/main.c
//
// This is intentionally a simple text format. No magic numbers, no
// binary parsing. The focus is on the staging area CONCEPT (tracking
// what will go into the next commit) and ATOMIC WRITES (temp+rename).
//
// PROVIDED functions: index_find, index_remove, index_status
// TODO functions:     index_load, index_save, index_add

#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

// Find an index entry by path (linear scan).
IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

// Remove a file from the index.
// Returns 0 on success, -1 if path not in index.
int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

// Print the status of the working directory.
//
// Identifies files that are staged, unstaged (modified/deleted in working dir),
// and untracked (present in working dir but not in index).
// Returns 0.
int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;
    // Note: A true Git implementation deeply diffs against the HEAD tree here. 
    // For this lab, displaying indexed files represents the staging intent.
    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged_count = 0;
    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            // Fast diff: check metadata instead of re-hashing file content
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec || st.st_size != (off_t)index->entries[i].size) {
                printf("  modified:   %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }
    if (unstaged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked_count = 0;
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            // Skip hidden directories, parent directories, and build artifacts
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue; // compiled executable
            if (strstr(ent->d_name, ".o") != NULL) continue; // object files

            // Check if file is tracked in the index
            int is_tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    is_tracked = 1; 
                    break;
                }
            }
            
            if (!is_tracked) {
                struct stat st;
                stat(ent->d_name, &st);
                if (S_ISREG(st.st_mode)) { // Only list regular files for simplicity
                    printf("  untracked:  %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }
        closedir(dir);
    }
    if (untracked_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    return 0;
}

// ─── TODO: Implement these ───────────────────────────────────────────────────

static int compare_entries(const void *a, const void *b) {
    const IndexEntry *entry_a = (const IndexEntry *)a;
    const IndexEntry *entry_b = (const IndexEntry *)b;
    return strcmp(entry_a->path, entry_b->path);
}


// Save the index to .pes/index atomically.



// Load the index from .pes/index.
//
// HINTS - Useful functions:
//   - fopen (with "r"), fscanf, fclose : reading the text file line by line
//   - hex_to_hash                      : converting the parsed string to ObjectID
//
// Returns 0 on success, -1 on error.
// Load the index from .pes/index.
int index_load(Index *index) {
    index->count = 0;
    
    char path[512];
    snprintf(path, sizeof(path), "%s/index", PES_DIR);

    FILE *f = fopen(path, "r");
    if (!f) {
        // It's not an error if the index file doesn't exist yet (e.g., brand new repo).
        return 0; 
    }

    char line[1024];
    while (fgets(line, sizeof(line), f) != NULL) {
        if (index->count >= MAX_INDEX_ENTRIES) {
            fprintf(stderr, "error: maximum index size exceeded\n");
            fclose(f);
            return -1;
        }

        IndexEntry *entry = &index->entries[index->count];
        char hex_hash[HASH_HEX_SIZE + 1];

        // Parse: <mode-octal> <64-char-hex-hash> <mtime-seconds> <size> <path>
        if (sscanf(line, "%o %64s %llu %zu %255[^\n]",
                   &entry->mode,
                   hex_hash,
                   &entry->mtime_sec,
                   &entry->size,
                   entry->path) != 5) {
            fprintf(stderr, "warning: invalid entry in index, skipping\n");
            continue;
        }

        if (hex_to_hash(hex_hash, &entry->id) != 0) {
            fprintf(stderr, "warning: invalid hash format in index, skipping\n");
            continue;
        }

        index->count++;
    }

    fclose(f);
    return 0;
}

// Save the index to .pes/index atomically.
//
// HINTS - Useful functions and syscalls:
//   - qsort                            : sorting the entries array by path
//   - fopen (with "w"), fprintf        : writing to the temporary file
//   - hash_to_hex                      : converting ObjectID for text output
//   - fflush, fileno, fsync, fclose    : flushing userspace buffers and syncing to disk
//   - rename                           : atomically moving the temp file over the old index
//
// Returns 0 on success, -1 on error.
int index_save(const Index *index) {
    // 1. Sort the entries (Git requires sorted index entries for tree generation)
    // We cast away const to sort in-place, assuming the caller allows it. 
    // In a strict implementation, we'd copy it, but this is fine for this lab.
    qsort((void*)index->entries, index->count, sizeof(IndexEntry), compare_entries);

    // 2. Open a temporary file
    char temp_path[512];
    snprintf(temp_path, sizeof(temp_path), "%s/index.tmp", PES_DIR);
    
    FILE *f = fopen(temp_path, "w");
    if (!f) return -1;

    // 3. Write each entry to the temp file
    for (int i = 0; i < index->count; i++) {
        const IndexEntry *entry = &index->entries[i];
        char hex_hash[HASH_HEX_SIZE + 1];
        hash_to_hex(&entry->id, hex_hash);

        if (fprintf(f, "%o %s %llu %zu %s\n",
                    entry->mode,
                    hex_hash,
                    entry->mtime_sec,
                    entry->size,
                    entry->path) < 0) {
            fclose(f);
            unlink(temp_path);
            return -1;
        }
    }

    // 4. Flush and sync to disk to ensure data integrity
    if (fflush(f) != 0 || fsync(fileno(f)) < 0) {
        fclose(f);
        unlink(temp_path);
        return -1;
    }
    fclose(f);

    // 5. Atomically rename the temp file to the real index file
    char final_path[512];
    snprintf(final_path, sizeof(final_path), "%s/index", PES_DIR);
    
    if (rename(temp_path, final_path) < 0) {
        unlink(temp_path);
        return -1;
    }

    return 0;
}
// Stage a file for the next commit.
//
// HINTS - Useful functions and syscalls:
//   - fopen, fread, fclose             : reading the target file's contents
//   - object_write                     : saving the contents as OBJ_BLOB
//   - stat / lstat                     : getting file metadata (size, mtime, mode)
//   - index_find                       : checking if the file is already staged
//
// Returns 0 on success, -1 on error.
// A helper comparison function for sorting index entries by path.

// Stage a file for the next commit.
int index_add(Index *index, const char *path) {
    // 1. Get file metadata (stat)
    struct stat st;
    if (lstat(path, &st) != 0) {
        fprintf(stderr, "error: could not stat '%s'\n", path);
        return -1;
    }

    // Only allow regular files and executables for now
    if (!S_ISREG(st.st_mode)) {
        fprintf(stderr, "error: '%s' is not a regular file\n", path);
        return -1;
    }

    // 2. Read the file's entire contents into memory
    FILE *f = fopen(path, "rb");
    if (!f) {
        fprintf(stderr, "error: could not open '%s'\n", path);
        return -1;
    }

    void *file_data = NULL;
    if (st.st_size > 0) {
        file_data = malloc(st.st_size);
        if (!file_data) {
            fclose(f);
            return -1;
        }
        if (fread(file_data, 1, st.st_size, f) != (size_t)st.st_size) {
            free(file_data);
            fclose(f);
            return -1;
        }
    }
    fclose(f);

    // 3. Write the contents into the object store as a blob
    ObjectID blob_id;
    if (object_write(OBJ_BLOB, file_data, st.st_size, &blob_id) != 0) {
        fprintf(stderr, "error: failed to write object for '%s'\n", path);
        if (file_data) free(file_data);
        return -1;
    }
    if (file_data) free(file_data);

    // 4. Update the index entry
    IndexEntry *entry = index_find(index, path);
    if (!entry) {
        // If it's a new file, add it to the end of the array
        if (index->count >= MAX_INDEX_ENTRIES) {
            fprintf(stderr, "error: index full\n");
            return -1;
        }
        entry = &index->entries[index->count++];
        strncpy(entry->path, path, sizeof(entry->path) - 1);
        entry->path[sizeof(entry->path) - 1] = '\0';
    }

    // Update the metadata for the entry
    entry->id = blob_id;
    entry->size = st.st_size
    entry->mtime_sec = (unsigned long long)st.st_mtime;
    
    // Convert system mode to Git-style mode (executable vs regular)
    if (st.st_mode & S_IXUSR) {
        entry->mode = 0100755; // Executable
    } else {
        entry->mode = 0100644; // Regular file
    }

    // 5. Save the updated index back to disk
    return index_save(index);
}
