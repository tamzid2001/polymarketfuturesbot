# Repository source-line inventory

Counted with cloc 2.06, using the tracked working tree for the statistical
reporting update on top of production commit `80965a3751`. Source-only counts
include Python, YAML, shell, JavaScript and TypeScript; they exclude CSV/JSON
datasets, Markdown, downloaded artifacts, virtual environments, Git object
history, and duplicate worktree checkouts. Identical source files at distinct
tracked paths are counted separately (`--skip-uniqueness`).

| Category | Files | Code lines | Comment/docstring lines | Blank lines |
| --- | ---: | ---: | ---: | ---: |
| Non-archived source, excluding tests/workflows | 32 | 20,999 | 2,214 | 1,867 |
| Non-archived tests | 21 | 5,827 | 108 | 651 |
| Current workflow definitions | 11 | 1,056 | 71 | 100 |
| Archived source/tests/workflows | 55 | 16,508 | 1,097 | 1,985 |
| **Total** | **119** | **44,390** | **3,490** | **4,603** |

Thus the non-archived subtotal including tests and workflows is **27,882 code
lines**; all counted physical lines including comments and blanks total
**52,483**. Non-archived does not mean every module or workflow is currently
executing in production. The archived 16,508 lines are not part of the current
runner and should not be represented as active trading logic.

Classification: `archive/` takes precedence; tests include `tests/`, nested
`*/tests/`, and root `test_*.py`; current workflows are `.github/workflows/`.
The remaining counted, non-archived files form the source category.

Command (from repository root):

```sh
cloc --vcs=git --skip-uniqueness --include-lang='Python,YAML,Bourne Shell,JavaScript,TypeScript' --by-file --json
```

Per-file counts are saved in `source_inventory.json`. Line count is a size
measure, not a valuation, security certification, or proof of profitability.
