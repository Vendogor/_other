import os
import fnmatch

MAX_DEPTH = 5

def read_gitignore(path=".gitignore"):
    patterns = []
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"): patterns.append(line)
    return patterns

def is_ignored(rel_path, patterns):
    for pattern in patterns:
        if fnmatch.fnmatch(rel_path, pattern) or fnmatch.fnmatch(os.path.basename(rel_path), pattern): return True
    return False

def color_text(text, is_dir):
    if is_dir: return f"\033[34m📁 {text}\033[0m"  # Синие папки
    else: return f"\033[32m📄 {text}\033[0m"  # Зелёные файлы

def print_tree(path=".", prefix="", depth=0, ignore_patterns=[]):
    if depth >= MAX_DEPTH: return

    entries = sorted(os.listdir(path))
    for i, entry in enumerate(entries):
        full_path = os.path.join(path, entry)
        rel_path = os.path.relpath(full_path)
        if is_ignored(rel_path, ignore_patterns) or entry == ".git": continue
        connector = "└── " if i == len(entries) - 1 else "├── "
        display_name = color_text(entry, os.path.isdir(full_path))
        print(prefix + connector + display_name)
        if os.path.isdir(full_path):
            extension = "    " if i == len(entries) - 1 else "│   "
            print_tree(full_path, prefix + extension, depth + 1, ignore_patterns)

if __name__ == "__main__":
    ignore = read_gitignore()
    print_tree(".", "", 0, ignore)
