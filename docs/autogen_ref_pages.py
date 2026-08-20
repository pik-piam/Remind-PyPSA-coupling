from pathlib import Path
import mkdocs_gen_files

# Set the root and scripts directory
root = Path(__file__).parent.parent
docu_dir = root / "docs"
src_dir = root / "src" / "iampypsa/"

nav_entries: list[tuple[str, Path]] = []

# Loop through all Python files in the `src_dir` using rglob
for path in sorted(src_dir.rglob("[!_]*.py")):
    # Generate the module path relative to the scripts directory, excluding the '.py' extension
    module_path = path.relative_to(src_dir).with_suffix("")

    # Generate the corresponding markdown file path, replacing '.py' with '.md'
    doc_path = path.relative_to(src_dir).with_suffix(".md")

    # Final path for the documentation, placed in the `reference` folder
    full_doc_path = Path("docs/reference", doc_path)

    # Extract module parts
    parts = tuple(module_path.parts)

    # Remove '__init__' or '__main__' from module path if present
    if parts[-1] == "__init__":
        parts = parts[:-1]
    elif parts[-1] == "__main__":
        continue  # Skip '__main__' files as they don't need documentation

    # Create the documentation file
    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        identifier = ".".join(parts)
        print(f"::: {identifier}", file=fd)  # MkDocs-specific syntax to pull in the module

    # Set the edit path to link the documentation back to the original Python file
    mkdocs_gen_files.set_edit_path(full_doc_path, path.relative_to(root))

    # Dotted module path (e.g. "models.remind.coupler") as the nav title — unique across the whole
    # package, unlike the bare leaf filename (both formats/iamc.py and models/iamc/coupler.py would
    # otherwise collide on the title "base").
    nav_entries.append((".".join(parts), doc_path))

# Scoped inside docs/reference/ (not the docs root!): a nav file at the docs root would make
# literate-nav take over — and silently ignore — the *entire* site nav (see its "hybrid nav"
# docs). mkdocs.yml references this subtree via `- Docs: docs/reference/`.
with mkdocs_gen_files.open("docs/reference/reference_nav.yml", "w") as nav_file:
    for title, doc_path in sorted(nav_entries):
        # A proper Markdown link, not a bare filename — literate-nav requires link syntax
        # (or a `*.md` glob) for a scoped/hybrid nav file.
        nav_file.write(f"- [{title}]({doc_path.as_posix()})\n")
