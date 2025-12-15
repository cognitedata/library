# cdf_infield_location

This module contains location-specific configurations for Infield Quickstart. This is a simplified setup designed for quick deployment.

## Overview

The Infield Quickstart module provides a streamlined configuration for setting up Infield with minimal dependencies. Unlike the full Infield module, this quickstart version:

- Uses a single auth group (normal users) instead of multiple roles
- Does not include data transformation pipelines
- Uses a simplified configuration structure
- Focuses on getting Infield up and running quickly

## Configuration

The module uses the `first_location` variable (set in the parent `default.config.yaml`) to configure location-specific resources. By default, this is set to `oid`.

In [./default.config.yaml](default.config.yaml), you will find configuration values that need to be updated:

- `infield_location_normal_users_source_id`: The source ID from your identity provider for the normal users group

## App Config

This module creates a configuration instance of Infield by populating the APM_Config data model with a configuration. This configuration ties together groups, spaces, and location filters for a specific Infield location.

The configuration is defined in:
- [InFieldLocationConfig](./cdf_applications/infield_quickstart.InFieldLocationConfig.yaml) - Application configuration with feature toggles
- [LocationFilter](./locations/infield_quickstart.LocationFilter.yaml) - Location filter defining data models, instance spaces, and views

## Auth

The module creates one group that needs a matching group in the identity provider that the CDF project is configured with:

* **Normal role** (`gp_infield_{{first_location}}_normal_users`) - For regular Infield users who can execute checklists and upload media

The source ID from the group in the identity provider should be set in [./default.config.yaml](default.config.yaml) as `infield_location_normal_users_source_id`.

## Data Models

There are three spaces created across the quickstart modules:

1. **Common spaces** (in `cdf_infield_common` module):
   - `cognite_app_data` - Used to store user and user preference data
   - `APM_Config` - Used to store application configurations

2. **Location-specific spaces** (in this module):
   - `sp_infield_{{first_location}}_app_data` - Space for Infield to store app data (e.g., uploaded images from Infield)
   - `sp_asset_{{first_location}}_source` - Space for data from source systems (e.g., assets, activities/work orders)

## Location Filter

The LocationFilter configuration defines:
- **Data Models**: Core data models used (CogniteCore and CogniteProcessIndustries)
- **Instance Spaces**: Source space and app data space (source space must be listed first)
- **Views**: Views for Maintenance Orders, Operations, Notifications, and Assets

## Implementation Notes

* The quickstart module does not include data transformation pipelines. You will need to ensure your asset hierarchy and work orders are already available in the appropriate data models and spaces.
* The `rootLocationExternalId` in InFieldLocationConfig must match the `externalId` of the LocationFilter.
* The source space (`sp_asset_{{first_location}}_source`) must be defined first in the `instanceSpaces` list in the LocationFilter.
* All space and group names use the `{{first_location}}` template variable, which is set in the parent module's `default.config.yaml`.