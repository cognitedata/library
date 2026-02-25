# CDF Data Model Documentation App – Product Requirements

## 1.1 Purpose
Build and publish documentation of CDF Data Models stored in Cognite Data Fusion. The app lets users select a data model by space, externalId, and version, then view NEAT-style documentation (views, properties, inheritance, ER-style diagrams) with Dune template and styling.

## 1.2 Scope
- List and retrieve CDF Data Models from the connected CDF project.
- User selects one data model (space, externalId, version) for documentation.
- Generate documentation with the same information and functionality as the NEAT YAML–based generator: model metadata, view cards by category, property tables (own + inherited), direct relations, search, and Mermaid ER diagrams.
- Apply Dune/Aura design system and template.

## 1.3 Target Audience
Data modelers, engineers, and stakeholders who need to explore and document CDF Data Models in Cognite Data Fusion.

## 2. Functional Requirements
- **FR1** List available data models (optionally filtered by space).
- **FR2** Select a data model by space, externalId, and version (from list or manual input).
- **FR3** Retrieve the selected data model with inline view definitions from CDF.
- **FR4** Display documentation: model name, description, stats (views, properties, relations).
- **FR5** Categorize views by domain (simplified industry-domain logic).
- **FR6** View cards with expand/collapse, property tables (View Property, Container Property, Type, Cardinality, Description), inherited properties grouped by source.
- **FR7** Search/filter views and properties.
- **FR8** Mermaid class diagrams for ER-style visualization.
- **FR9** Theme toggle (light/dark) consistent with Dune.

## 3. Non-Functional Requirements
- **NFR1** Use @cognite/dune for auth and @cognite/sdk for CDF APIs.
- **NFR2** Use Dune/Aura and shadcn components where applicable.
- **NFR3** Responsive layout.

## 4. Data Models & Integration
### 4.1 CDF Data Model
- **4.1.1** Use CDF Data Models API: list data models, retrieve by (space, externalId, version) with inline views.
- **4.1.2** Transform CDF ViewDefinition (and containers where needed) into internal doc model: views, properties per view, direct relations.
- **4.1.3** Spaces: any space the user has read access to.

## 5. User Stories
- As a data modeler, I want to see all data models in my CDF project so I can choose which one to document.
- As a user, I want to select a data model by space, externalId, and version and see full documentation so I can understand its structure.
- As a user, I want to search views and properties and expand view cards so I can find details quickly.
- As a user, I want to see ER-style diagrams and light/dark theme so the doc is readable and consistent with Dune.
