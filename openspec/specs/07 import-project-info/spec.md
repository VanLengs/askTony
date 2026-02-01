# import-project-info Specification

## Purpose
TBD - created by archiving change analyze-project-activity. Update Purpose after archive.
## Requirements
### Requirement: Import project master data from CSV
The system SHALL provide a CLI command to import project master data into the warehouse.

#### Scenario: Import projects successfully
- **WHEN** the user runs `asktony import-project-info --project-file <csv>`
- **THEN** the system upserts `project_id` and related fields into `gold.dim_project`
- **THEN** the command prints import statistics and exits with code 0

#### Scenario: Missing required columns
- **WHEN** the provided project CSV is missing required columns
- **THEN** the command fails validation and reports the missing column names

### Requirement: Project ID is derived from project name pinyin
When `project_id` is not provided in the project CSV, the system MUST derive `project_id` from `project_name` by converting to pinyin using the operating system built-in transliteration and then normalizing to a stable identifier.

#### Scenario: Derive project_id for Chinese project name
- **WHEN** a project row has `project_name='鸿溟平台4.0项目'` and empty `project_id`
- **THEN** the system generates a deterministic `project_id` based on OS transliteration + normalization
- **THEN** the generated `project_id` is lowercase and contains only `[a-z0-9_]`

#### Scenario: Transliteration is unavailable
- **WHEN** OS transliteration is unavailable on the current platform
- **THEN** the system MUST require explicit `project_id` in the CSV and fail validation otherwise

### Requirement: Import project-to-repo mapping with time range and weight
The system SHALL support importing project-to-repo mappings with effective time ranges and weights.

#### Scenario: Import project-repo mapping successfully
- **WHEN** the user runs `asktony import-project-info --project-repo-file <csv>`
- **THEN** the system upserts mappings into `gold.bridge_project_repo`

#### Scenario: Enforce required mapping fields
- **WHEN** a mapping row is missing `project_id` or `repo_id`
- **THEN** the system reports a validation issue for that row

#### Scenario: Validate mapping weight range
- **WHEN** a mapping row has `weight <= 0` or `weight > 1`
- **THEN** the system reports a validation issue for that row

#### Scenario: Validate non-overlapping ranges per (project_id, repo_id)
- **WHEN** the same `(project_id, repo_id)` appears with overlapping date ranges
- **THEN** the system reports a validation issue for that overlap

### Requirement: Import project members by employee_id with role and allocation
The system SHALL support importing project member assignments using `employee_id` as the person identifier, with roles and allocation (FTE).

#### Scenario: Import project members successfully
- **WHEN** the user runs `asktony import-project-info --project-member-file <csv>`
- **THEN** the system upserts rows into `gold.bridge_project_person_role`

#### Scenario: Enforce employee_id presence
- **WHEN** a project member row has empty `employee_id`
- **THEN** the system reports a validation issue for that row

#### Scenario: Validate allocation range
- **WHEN** a project member row has `allocation <= 0` or `allocation > 1`
- **THEN** the system reports a validation issue for that row

### Requirement: Support dry-run validation
The system SHALL support a dry-run mode that performs validation and prints issues without modifying the database.

#### Scenario: Dry-run does not write
- **WHEN** the user runs `asktony import-project-info --dry-run ...`
- **THEN** the system performs all validations and prints issues/stats
- **THEN** the system performs no database writes

### Requirement: Export templates for project CSVs
The system SHOULD provide template export commands (or documented template files) for the required CSV formats.

#### Scenario: User exports template
- **WHEN** the user requests a template export for a project input CSV
- **THEN** the system outputs a CSV with correct headers and example rows (or empty rows)

