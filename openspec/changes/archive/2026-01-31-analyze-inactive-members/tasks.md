## 1. Warehouse Query

- [x] 1.1 Add `Warehouse.inactive_members_data(months: int, top: int|None)` returning `(columns, rows)` with required columns and `commit_count=0`
- [x] 1.2 Use window filtering consistent with existing analyze methods (`commit_month >= since_month` and `committed_at >= since_ts`)
- [x] 1.3 Base member set on `gold.bridge_repo_member` distinct `member_key` and compute inactive via set difference against window activity (join on `repo_id + member_key`)
- [x] 1.4 Add `Warehouse.inactive_members(months: int, top: int)` returning a rich table (consistent with other analyze outputs)

## 2. CLI Command

- [x] 2.1 Add `asktony analyze inactive-members` command in `src/asktony/commands/analyze.py` with options `--months`, `--top`, `--all`, `--csv`
- [x] 2.2 Reuse existing `_write_csv` to export UTF-8 CSV with header and no truncation
- [x] 2.3 When `--csv` is set, filter exported rows to only include records whose `full_name` is not empty/blank (use `strip()`), and print export summary (`csv` path + row count)
- [x] 2.4 When `--csv` is not set, print rich table output from warehouse method

## 3. Documentation

- [x] 3.1 Update `README.md` “常用分析与导出” section to include examples for `asktony analyze inactive-members` and CSV export usage
- [x] 3.2 Mention prerequisite that `full_name` comes from `import-dim-info` (CSV export filters out rows without `full_name`)

## 4. Verification

- [x] 4.1 Run `python -m asktony --help` / `asktony analyze --help` to ensure command is registered and help text renders
- [ ] 4.2 Run the new command against an existing local DB (if present) to confirm it returns results and CSV export works
