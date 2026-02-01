## ADDED Requirements

### Requirement: Analyze project activity in a month window
The system SHALL provide an analysis command to summarize project activity and efficiency for the last N months.

#### Scenario: User runs project activity analysis
- **WHEN** the user runs `asktony analyze project-activity --months 2`
- **THEN** the system outputs a table of project metrics for the window

### Requirement: Project activity output includes resource and output metrics
The project activity analysis MUST include metrics that reflect both resource usage and contribution.

#### Scenario: Output includes core metrics
- **WHEN** `asktony analyze project-activity` runs successfully
- **THEN** each project row includes at least:
  - `project_id` and `project_name`
  - `dev_headcount` and `dev_fte_sum`
  - `weighted_commits_total`
  - `weighted_commits_per_fte`
  - `active_dev` / `inactive_dev` and `active_pct`

### Requirement: Project analysis supports CSV export
The system SHALL support exporting project analysis results to CSV.

#### Scenario: Export to CSV
- **WHEN** the user runs `asktony analyze project-activity --months 2 --csv output/project_activity.csv`
- **THEN** the system writes a CSV file with headers and all rows

### Requirement: Project analysis accounts for multi-project repo weights
When a repo is mapped to multiple projects in the window, the analysis MUST use weights to split commit contributions.

#### Scenario: Weighted attribution affects totals
- **WHEN** a repo is mapped to two projects with weights 0.7 and 0.3 for the same month
- **THEN** the weighted commit totals reflect a 70/30 split for that repo's commits in that month

### Requirement: Project analysis uses employee_id as the member unit
The project analysis MUST use `employee_id` as the unique project member identifier for resource and activity counting.

#### Scenario: Single employee with multiple identities is counted once
- **WHEN** an employee appears multiple times in enrichment identities but shares the same `employee_id`
- **THEN** the analysis counts that employee once in headcount and active/inactive breakdown

### Requirement: Provide risk signals for concentration and coverage
The project analysis SHOULD include additional signals to support governance and improvement actions.

#### Scenario: Output includes risk signals
- **WHEN** `asktony analyze project-activity` runs successfully
- **THEN** the output includes at least one concentration metric (e.g., top1 share) and one coverage metric (e.g., role coverage)

