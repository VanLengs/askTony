## 1. Warehouse Query

- [x] 1.1 Add `Warehouse.missing_fullname_authors_data(months: int, top: int|None)` returning `(columns, rows)` with required columns
- [x] 1.2 Use window filtering consistent with existing analyze methods (`commit_month >= since_month` and `committed_at >= since_ts`)
- [x] 1.3 Filter to authors with `full_name` empty/blank and `commit_count > 0`
- [x] 1.4 Add `Warehouse.missing_fullname_authors(months: int, top: int|None)` returning a rich table

## 2. CLI Command

- [x] 2.1 Add `asktony analyze missing-fullname-authors` command in `src/asktony/commands/analyze.py` with options `--months`, `--top`, `--all`, `--csv`
- [x] 2.2 Reuse existing `_write_csv` to export UTF-8 CSV with header and no truncation
- [x] 2.3 When `--csv` is not set, print rich table output from warehouse method

## 3. Documentation

- [x] 3.1 Update `README.md` “常用分析与导出” to include examples for `asktony analyze missing-fullname-authors` and CSV export usage
- [x] 3.2 Briefly explain it helps identify members to fill `full_name` for `import-dim-info`

## 4. Verification

- [x] 4.1 Run `asktony analyze --help` to ensure `missing-fullname-authors` is registered
- [ ] 4.2 Run the new command against an existing local DB (if present) to confirm output and CSV export work
