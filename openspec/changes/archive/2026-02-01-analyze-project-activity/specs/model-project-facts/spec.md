## ADDED Requirements

### Requirement: Create project dimension and bridge tables in gold
The system SHALL create and maintain project-related tables/views in the `gold` schema.

#### Scenario: Build creates project tables
- **WHEN** the user runs the build/model pipeline
- **THEN** `gold.dim_project`, `gold.bridge_project_repo`, and `gold.bridge_project_person_role` exist and are queryable

### Requirement: Resolve commits to employees (employee_id) consistently
The system MUST resolve commit authors to employees using the existing employee roster and identity alignment logic, and then map to `employee_id`.

#### Scenario: Author resolves by member_key
- **WHEN** a commit has a `member_key` that matches an employee identity
- **THEN** the commit is attributed to that employee's `employee_id`

#### Scenario: Author resolves by email/username fallback
- **WHEN** a commit member_key does not directly match an employee but `author_email` or `author_username` matches
- **THEN** the system attributes the commit to the matched employee's `employee_id`

### Requirement: Attribute commits to projects via repo mapping with weight
The system SHALL attribute commits to projects through `bridge_project_repo`, supporting multi-project attribution via weights.

#### Scenario: Commit attributed to a single project
- **WHEN** a repo has exactly one active mapping to a project for that month
- **THEN** the commit contributes weight 1.0 to that project

#### Scenario: Commit split across multiple projects
- **WHEN** a repo has multiple active mappings for the same month with weights
- **THEN** the commit's contribution is split across projects proportionally by `weight`

### Requirement: Compute project-person-month and project-month aggregates
The system SHALL compute monthly aggregates for projects and project members.

#### Scenario: Build creates monthly aggregates
- **WHEN** the build/model pipeline runs successfully
- **THEN** the system provides a project-month aggregate (project × month)
- **THEN** the system provides a project-person-month aggregate (project × employee_id × month)

### Requirement: Apply project member roster and allocation for resource metrics
The system SHALL use `bridge_project_person_role` to compute project resource metrics (headcount and FTE), and SHALL support allocation (FTE) weighting.

#### Scenario: Compute headcount and FTE
- **WHEN** a project has 4 members with allocations summing to 2.5
- **THEN** the project-month metrics include `dev_headcount=4` and `dev_fte_sum=2.5`

### Requirement: Use month grain as primary time axis
The system MUST use `commit_month` as the primary time grain for project activity and efficiency metrics in the MVP.

#### Scenario: Month-based windowing
- **WHEN** the user queries project activity for the last N months
- **THEN** the computation uses `commit_month >= since_month` and corresponding timestamp constraints

