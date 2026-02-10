## 1.1 Purpose
Provide a lightweight UI that helps users discover and assess CDF data models.

## 1.2 Scope
- Read-only listing of Cognite Data Models available in the current CDF project.
- Tile-based UI for quick scanning and selection.

## 1.3 Target Audience
- Data engineers and solution architects working with CDF data models.
- Developers exploring available schemas for integration work.

## 2. Functional Requirements
- Fetch all available CDF data models (including global/system).
- Display each model as a clickable tile with key metadata.
- Show loading, error, and empty states.

## 3. Non-Functional Requirements
- Responsive layout for desktop and tablet.
- Accessible keyboard interaction for tiles.
- Fast initial render with clear loading feedback.

## 4. Data Models & Integration
- Use Cognite Data Modeling SDK via the Dune SDK client.

### 4.1 CDF Data Model
#### 4.1.1 Existing views
- Leverage existing data model definitions returned by CDF.

#### 4.1.2 New views
- None required for the listing UI.

#### 4.1.3 Spaces
- Read across all accessible spaces, including global/system spaces.

## 5. User Stories
- As a data engineer, I want to see all available data models so I can choose the right schema.
- As an architect, I want a quick overview of data models to understand what exists in the project.
