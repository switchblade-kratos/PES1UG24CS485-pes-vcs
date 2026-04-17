// tree.c — Tree object serialization and construction
//
// PROVIDED functions: get_file_mode, tree_parse, tree_serialize
// TODO functions:     tree_from_index
//
// Binary tree format (per entry, concatenated with no separators):
//   "<mode-as-ascii-octal> <name>\0<32-byte-binary-hash>"
//
// Example single entry (conceptual):
//   "100644 hello.txt\0" followed by 32 raw bytes of SHA-256


#include "tree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

// ─── Mode Constants ─────────────────────────────────────────────────────────

#define MODE_FILE      0100644
#define MODE_EXEC      0100755
#define MODE_DIR       0040000

// ─── PROVIDED ───────────────────────────────────────────────────────────────

// Determine the object mode for a filesystem path.
uint32_t get_file_mode(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0) return 0;

    if (S_ISDIR(st.st_mode))  return MODE_DIR;
    if (st.st_mode & S_IXUSR) return MODE_EXEC;
    return MODE_FILE;
}

// Parse binary tree data into a Tree struct safely.
// Returns 0 on success, -1 on parse error.
int tree_parse(const void *data, size_t len, Tree *tree_out) {
    tree_out->count = 0;
    const uint8_t *ptr = (const uint8_t *)data;
    const uint8_t *end = ptr + len;

    while (ptr < end && tree_out->count < MAX_TREE_ENTRIES) {
        TreeEntry *entry = &tree_out->entries[tree_out->count];

        // 1. Safely find the space character for the mode
        const uint8_t *space = memchr(ptr, ' ', end - ptr);
        if (!space) return -1; // Malformed data

        // Parse mode into an isolated buffer
        char mode_str[16] = {0};
        size_t mode_len = space - ptr;
        if (mode_len >= sizeof(mode_str)) return -1;
        memcpy(mode_str, ptr, mode_len);
        entry->mode = strtol(mode_str, NULL, 8);

        ptr = space + 1; // Skip space

        // 2. Safely find the null terminator for the name
        const uint8_t *null_byte = memchr(ptr, '\0', end - ptr);
        if (!null_byte) return -1; // Malformed data

        size_t name_len = null_byte - ptr;
        if (name_len >= sizeof(entry->name)) return -1;
        memcpy(entry->name, ptr, name_len);
        entry->name[name_len] = '\0'; // Ensure null-terminated

        ptr = null_byte + 1; // Skip null byte

        // 3. Read the 32-byte binary hash
        if (ptr + HASH_SIZE > end) return -1; 
        memcpy(entry->hash.hash, ptr, HASH_SIZE);
        ptr += HASH_SIZE;

        tree_out->count++;
    }
    return 0;
}

// Helper for qsort to ensure consistent tree hashing
static int compare_tree_entries(const void *a, const void *b) {
    return strcmp(((const TreeEntry *)a)->name, ((const TreeEntry *)b)->name);
}

// Serialize a Tree struct into binary format for storage.
// Caller must free(*data_out).
// Returns 0 on success, -1 on error.
int tree_serialize(const Tree *tree, void **data_out, size_t *len_out) {
    // Estimate max size: (6 bytes mode + 1 byte space + 256 bytes name + 1 byte null + 32 bytes hash) per entry
    size_t max_size = tree->count * 296; 
    uint8_t *buffer = malloc(max_size);
    if (!buffer) return -1;

    // Create a mutable copy to sort entries (Git requirement)
    Tree sorted_tree = *tree;
    qsort(sorted_tree.entries, sorted_tree.count, sizeof(TreeEntry), compare_tree_entries);

    size_t offset = 0;
    for (int i = 0; i < sorted_tree.count; i++) {
        const TreeEntry *entry = &sorted_tree.entries[i];
        
        // Write mode and name (%o writes octal correctly for Git standards)
        int written = sprintf((char *)buffer + offset, "%o %s", entry->mode, entry->name);
        offset += written + 1; // +1 to step over the null terminator written by sprintf
        
        // Write binary hash
        memcpy(buffer + offset, entry->hash.hash, HASH_SIZE);
        offset += HASH_SIZE;
    }

    *data_out = buffer;
    *len_out = offset;
    return 0;
}

// ─── TODO: Implement these ──────────────────────────────────────────────────

// Build a tree hierarchy from the current index and write all tree
// objects to the object store.
//
// HINTS - Useful functions and concepts for this phase:
//   - index_load      : load the staged files into memory
//   - strchr          : find the first '/' in a path to separate directories from files
//   - strncmp         : compare prefixes to group files belonging to the same subdirectory
//   - Recursion       : you will likely want to create a recursive helper function 
//                       (e.g., `write_tree_level(entries, count, depth)`) to handle nested dirs.
//   - tree_serialize  : convert your populated Tree struct into a binary buffer
//   - object_write    : save that binary buffer to the store as OBJ_TREE
//
// Returns 0 on success, -1 on error.
// Recursive helper to build trees from a sorted subset of index entries.
// 'prefix_len' tells us how deep we are in the directory structure so we can 
// ignore parent directory names when creating the local Tree entries.
static int write_tree_recursive(const IndexEntry *entries, int num_entries, int prefix_len, ObjectID *out_id) {
    Tree current_tree;
    current_tree.count = 0;

    int i = 0;
    while (i < num_entries && current_tree.count < MAX_TREE_ENTRIES) {
        // Look at the current file path, starting from our current directory depth
        const char *current_path = entries[i].path + prefix_len;
        const char *slash = strchr(current_path, '/');

        if (!slash) {
            // BASE CASE: It's a file in the current directory.
            TreeEntry *t_entry = &current_tree.entries[current_tree.count++];
            t_entry->mode = entries[i].mode;
            strncpy(t_entry->name, current_path, sizeof(t_entry->name) - 1);
            t_entry->name[sizeof(t_entry->name) - 1] = '\0';
            t_entry->hash = entries[i].id;
            i++; 
        } else {
            // RECURSIVE CASE: It's a file inside a subdirectory.
            // We need to group all index entries that belong to this same subdirectory.
            size_t dir_name_len = slash - current_path;
            
            int j = i + 1;
            // Advance 'j' until we find a file that does NOT belong in this subdirectory
            while (j < num_entries) {
                const char *next_path = entries[j].path + prefix_len;
                if (strncmp(current_path, next_path, dir_name_len) != 0 || next_path[dir_name_len] != '/') {
                    break;
                }
                j++;
            }

            // 'j - i' is the number of files inside this subdirectory.
            // We recursively call this function to build the tree for that subdirectory.
            ObjectID sub_tree_id;
            int new_prefix_len = prefix_len + dir_name_len + 1; // +1 to skip the slash
            
            if (write_tree_recursive(&entries[i], j - i, new_prefix_len, &sub_tree_id) != 0) {
                return -1;
            }

            // Now that the subdirectory tree is built and saved, add IT as an entry to our current tree.
            TreeEntry *t_entry = &current_tree.entries[current_tree.count++];
            t_entry->mode = MODE_DIR; // 0040000
            strncpy(t_entry->name, current_path, dir_name_len);
            t_entry->name[dir_name_len] = '\0';
            t_entry->hash = sub_tree_id;

            // Move the outer loop index past all the files we just processed in the subfolder
            i = j;
        }
    }

    // 1. Serialize the Tree struct into the binary format expected by Git/PES
    void *tree_data = NULL;
    size_t tree_len = 0;
    if (tree_serialize(&current_tree, &tree_data, &tree_len) != 0) {
        return -1;
    }

    // 2. Write the binary tree data to the object store
    int result = object_write(OBJ_TREE, tree_data, tree_len, out_id);
    free(tree_data);
    
    return result;
}


// Build a tree hierarchy from the current index and write all tree
// objects to the object store.
int tree_from_index(ObjectID *id_out) {
    Index idx;
    if (index_load(&idx) != 0) {
        return -1;
    }

    // If the index is empty, we can't create a tree
    if (idx.count == 0) {
        fprintf(stderr, "error: nothing to commit (index is empty)\n");
        return -1;
    }

    // The index should already be sorted by path (enforced in index_save),
    // which makes grouping subdirectories in write_tree_recursive possible.
    return write_tree_recursive(idx.entries, idx.count, 0, id_out);
}
