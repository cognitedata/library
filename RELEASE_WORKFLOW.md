# Release Workflow Documentation

## Overview

This repository uses GitHub Actions to automatically create releases when code is merged to the `main` branch. The workflow creates two types of releases:

1. **Timestamped Release**: A unique release with format `YYYYMMDD-short_hash` (e.g., `20241201-a1b2c3d`)
2. **Latest Release**: A canonical release tag called `latest` that always points to the most recent release

## How It Works

### Trigger
The workflow is triggered on:
- Push to `main` branch
- Merged pull requests to `main` branch

### Process
1. **Checkout**: Retrieves the full git history to access commit hashes
2. **Python Setup**: Sets up Python 3.11 environment
3. **Package Creation**: Runs `release_packages.py` to create `packages.zip`
4. **Version Generation**: Creates a timestamped version string (YYYYMMDD-short_hash)
5. **Release Creation**: Creates two GitHub releases:
   - Timestamped release with the version as tag
   - Latest release (updates existing if present)

## Release URLs

Both releases provide access to the same `packages.zip` file via these URLs:

- **Latest Release**: `https://github.com/{{organisation}}/{{repo}}/releases/download/latest/packages.zip`
- **Specific Release**: `https://github.com/{{organisation}}/{{repo}}/releases/download/{{timestamp-hash}}/packages.zip`

## Example

For a release created on December 1, 2024 with commit hash `a1b2c3d`:

- **Tag**: `20241201-a1b2c3d`
- **Latest Tag**: `latest` (updated to point to this release)
- **Download URLs**:
  - `https://github.com/{{org}}/{{repo}}/releases/download/20241201-a1b2c3d/packages.zip`
  - `https://github.com/{{org}}/{{repo}}/releases/download/latest/packages.zip`

## Benefits

- **Versioning**: Each release has a unique, sortable identifier
- **Stability**: The `latest` tag provides a consistent URL for the most recent release
- **Traceability**: Each release is tied to a specific commit
- **Automation**: No manual intervention required for releases

## Files

- **Workflow**: `.github/workflows/release-packages.yml`
- **Package Script**: `release_packages.py`
- **Output**: `packages.zip` (created during workflow execution)
