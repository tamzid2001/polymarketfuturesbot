# Repository source-line inventory

Counted with cloc 2.06, using the tracked working tree for the multi-series
replay update on top of production commit `9746358c72`. Source-only counts
include Python, YAML, shell, JavaScript and TypeScript; they exclude CSV/JSON
datasets, Markdown, downloaded artifacts, virtual environments, Git object
history, and duplicate worktree checkouts. Identical source files at distinct
tracked paths are counted separately (`--skip-uniqueness`).

| Category | Files | Code lines | Comment/docstring lines | Blank lines |
| --- | ---: | ---: | ---: | ---: |
| Non-archived source, excluding tests/workflows | 32 | 20,976 | 2,214 | 1,867 |
| Non-archived tests | 21 | 5,794 | 107 | 648 |
| Current workflow definitions | 11 | 1,056 | 71 | 100 |
| Archived source/tests/workflows | 55 | 16,508 | 1,097 | 1,985 |
| **Total** | **119** | **44,334** | **3,489** | **4,600** |

Thus the non-archived subtotal including tests and workflows is **27,826 code
lines**; all counted physical lines including comments and blanks total
**52,423**. Non-archived does not mean every module or workflow is currently
executing in production. The archived 16,508 lines are not part of the current
runner and should not be represented as active trading logic.

Command (from repository root):

```sh
cloc --vcs=git --skip-uniqueness --include-lang='Python,YAML,Bourne Shell,JavaScript,TypeScript' --by-file --json
```

Per-file counts are saved in `source_inventory.json`. Line count is a size
measure, not a valuation, security certification, or proof of profitability.
