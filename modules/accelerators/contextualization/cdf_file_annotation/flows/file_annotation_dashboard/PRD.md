# File Annotation Dashboard - Product Requirements Document

## 1. Introduction

### 1.1 Purpose
The File Annotation Dashboard is a Dune App (React-based) that provides comprehensive monitoring, analysis, and management capabilities for the File Annotation pipeline in Cognite Data Fusion (CDF). It enables users to understand annotation metrics, improve annotations, and monitor pipeline health.

### 1.2 Scope
This application covers two main dashboards:
1. **Annotation Quality Dashboard** - Metrics, per-file analysis, and pattern management
2. **Pipeline Health Dashboard** - KPIs, throughput, file debugging, and run history

### 1.3 Target Audience
- Data Engineers managing file annotation pipelines
- Operations teams monitoring pipeline health
- Data Scientists analyzing annotation coverage and quality

## 2. Functional Requirements

### 2.1 Annotation Quality Dashboard

#### 2.1.1 Overall Quality Metrics Tab
- Display overall annotation coverage percentage (actual vs. potential annotations)
- Show annotation coverage breakdown by:
  - Tag Entity Resource Type (bar chart)
  - File Resource Type (bar chart)
  - Secondary Scope property (bar chart)
- Provide tooltips with detailed counts and formulas

#### 2.1.2 Per-File Analysis Tab
- Filterable views by Resource Type and Secondary Scope
- File aggregation table showing:
  - File name, external ID, source ID
  - Resource type and secondary scope
  - Actual/Potential/Total annotation counts
  - Coverage percentage with progress bar
- Annotation comparison view:
  - Actual annotations table (ground truth)
  - Potential annotations table (pattern-detected, selectable)
- Coverage threshold metrics visualization (>=90%, 75-89%, 25-74%, <25%)

#### 2.1.3 Pattern Management Tab
- Manual Patterns catalog (editable)
  - Filter by Entity Type, Pattern Scope, Resource Type
  - Add/Edit/Delete patterns
  - Save and reset functionality
- Automatic Patterns catalog (read-only)
  - Filter by Entity Type, Pattern Scope, Resource Type
  - Display detected patterns

### 2.2 Pipeline Health Dashboard

#### 2.2.1 Overview Tab
- Live Pipeline KPIs:
  - Files Awaiting Processing
  - Total Files Processed
  - Overall Failure Rate (with failed file count)
- Pipeline Throughput chart:
  - Configurable time aggregation (Hourly, Daily, Weekly)
  - Bar chart showing files finalized over time

#### 2.2.2 File Explorer Tab
- Sortable/filterable file table with columns:
  - File name, last updated, external ID, source ID
  - Status (Awaiting, Annotated, Failed)
  - MIME type, page count, annotated page count
- Function Log Viewer for selected file:
  - Tabs for different function stages (Prepare, Launch, Finalize, Promote)
  - Relevant log entries and full log download

#### 2.2.3 Run History Tab
- Run summary metrics per function type:
  - Files Processed, Successful Runs, Failed Runs
- Run charts showing files processed per run type over time
- Detailed run history with:
  - Time window filter (All, Last 24h, Last 7 days, Last 30 days)
  - Status filter (All, Success, Failure)
  - Caller type filter (Prepare, Launch, Finalize, Promote)
  - Pagination for run entries
  - Function log viewer per run
  - Files processed per run

## 3. Non-Functional Requirements

### 3.1 Performance
- Initial load time < 3 seconds
- Data caching with reasonable TTL (1-2 hours for configuration, shorter for dynamic data)
- Pagination for large datasets

### 3.2 Usability
- Responsive design for different screen sizes
- Clear visual hierarchy and consistent styling
- Helpful tooltips and empty state messages
- Loading states for async operations

### 3.3 Accessibility
- Keyboard navigation support
- Screen reader compatible components
- Sufficient color contrast

## 4. Data Models & Integration

### 4.1 CDF Data Model

#### 4.1.1 Existing Views
- **AnnotationState View**: Tracks file annotation status and related function call IDs
  - Properties: linkedFile, annotationStatus, pageCount, annotatedPageCount, annotationMessage, patternModeMessage, launchFunctionId, launchFunctionCallId, finalizeFunctionId, finalizeFunctionCallId
- **File View**: File metadata from the configured HDM
  - Properties: name, sourceId, mimeType, resourceType (configurable), secondaryScope (configurable)

#### 4.1.2 Data Sources
- **Data Modeling Instances**: Primary source for annotation states and file metadata
- **Note**: Extraction Pipeline APIs, Functions APIs, and Raw Tables are NOT available in the Dune SDK (Data Modeling focused). The app will focus on Data Modeling-accessible data.

#### 4.1.3 Spaces
- Annotation State space (configured per pipeline)
- File space (configured per pipeline)

## 5. User Stories

### 5.1 Annotation Quality
1. As a Data Engineer, I want to see overall annotation coverage so I can understand the effectiveness of the annotation pipeline.
2. As a Data Engineer, I want to filter annotations by resource type so I can identify areas needing improvement.
3. As a Data Engineer, I want to manage manual patterns so I can improve annotation matching.
4. As a Data Engineer, I want to compare actual vs. potential annotations per file so I can identify promotion opportunities.

### 5.2 Pipeline Health
1. As an Operations Engineer, I want to see live KPIs so I can quickly assess pipeline health.
2. As an Operations Engineer, I want to see throughput trends so I can identify processing patterns.
3. As an Operations Engineer, I want to debug specific files so I can troubleshoot failures.
4. As an Operations Engineer, I want to view run history so I can track pipeline performance over time.

## 6. Technical Constraints

### 6.1 Dune Framework Limitations
The Dune framework provides a focused SDK optimized for Data Modeling use cases. The following CDF APIs are **NOT** available:
- Extraction Pipelines API (list pipelines, get config, run history)
- Functions API (get logs, call details)
- Raw Tables API (direct raw data access)

### 6.2 Adaptation Strategy
For the initial Dune implementation:
1. Pipeline selection will use a configuration-based approach (predefined views)
2. Annotation states and file metadata will be fetched from Data Modeling instances
3. Pipeline run history and function logs will be displayed as "Not available in Dune SDK" with placeholder for future integration
4. Manual pattern management will be read-only initially (requires Raw API)

## 7. UI/UX Guidelines

### 7.1 Design System
- Use shadcn/ui components for consistency
- Follow Cognite brand colors where applicable
- Use Recharts for data visualization

### 7.2 Layout
- Tabbed interface for main dashboard sections
- Card-based layouts for metrics and data displays
- Responsive grid for charts and tables
