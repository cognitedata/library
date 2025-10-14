# Contributing to CDF File Annotation Module

Thank you for your interest in contributing to the CDF File Annotation Module! This document outlines the process for contributing to this project.

## Contribution Workflow

All contributions to this project must follow this workflow:

### 1. Create a GitHub Issue

Before making any changes, please create a GitHub issue to discuss:

- **Bug Reports**: Describe the bug, steps to reproduce, expected vs. actual behavior, and your environment
- **Feature Requests**: Describe the feature, its use case, and how it would benefit the project
- **Documentation Improvements**: Describe what documentation is missing or needs clarification
- **Code Improvements**: Describe the refactoring or optimization you'd like to make

**Why create an issue first?**

- Ensures alignment on the problem and proposed solution
- Prevents duplicate work
- Allows for discussion before investing time in implementation
- Provides context for the eventual pull request

### 2. Create a Pull Request

Once the issue has been discussed and you're ready to contribute:

1. **Fork the repository** to your GitHub account
2. **Create a feature branch** from `main`:

   ```bash
   git checkout -b feature/issue-123-short-description
   ```

   or

   ```bash
   git checkout -b fix/issue-456-short-description
   ```

3. **Make your changes** following the code standards below

4. **Commit your changes** with clear, descriptive commit messages:

   ```bash
   git commit -m "Fix: Resolve cache invalidation issue (#123)

   - Updated cache validation logic to handle edge cases
   - Added unit tests for cache service
   - Updated documentation"
   ```

5. **Push to your fork**:

   ```bash
   git push origin feature/issue-123-short-description
   ```

6. **Create a Pull Request** on GitHub:
   - Reference the related issue in the PR description (e.g., "Closes #123" or "Fixes #456")
   - Provide a clear description of what changed and why
   - Include any relevant testing details or screenshots
   - Add `@dude-with-a-mug` as a reviewer (or the current maintainer)

### 3. Code Review and Approval

- **All PRs require approval** from the project maintainer (@dude-with-a-mug or designated reviewer) before merging
- The maintainer will review your code for:

  - Code quality and adherence to project standards
  - Test coverage
  - Documentation updates
  - Breaking changes or backward compatibility
  - Performance implications

- Address any feedback or requested changes
- Once approved, the maintainer will merge your PR

**Note**: PRs will not be merged without maintainer approval, even if all automated checks pass.

## Code Standards

### Python Code Style

- Use type hints for all function parameters and return values
- Maximum line length: 120 characters (as configured in the project)
- Use meaningful variable and function names

### Documentation

- **All functions must include Google-style docstrings** with:
  - Brief description
  - `Args`: Parameter descriptions
  - `Returns`: Return value description
  - `Raises`: Exception descriptions (if applicable)
- Update README.md or relevant documentation if your changes affect user-facing behavior
- Add inline comments for complex logic or non-obvious decisions

### Example Docstring Format

```python
def process_annotations(
    self,
    file_node: Node,
    regular_item: dict | None,
    pattern_item: dict | None
) -> tuple[str, str]:
    """
    Processes diagram detection results and applies annotations to a file.

    Handles both regular entity matching and pattern mode results, applying
    confidence thresholds and deduplication logic.

    Args:
        file_node: The file node instance to annotate.
        regular_item: Dictionary containing regular diagram detect results.
        pattern_item: Dictionary containing pattern mode results.

    Returns:
        A tuple containing:
            - Summary message of regular annotations applied
            - Summary message of pattern annotations created

    Raises:
        CogniteAPIError: If the API calls to apply annotations fail.
        ValueError: If the file node is missing required properties.
    """
    # Implementation...
```

### Testing

- Add tests for new functionality where applicable
- Ensure existing tests pass before submitting your PR
- Test locally using the VSCode debugger setup (see [DEPLOYMENT.md](DEPLOYMENT.md))

### Configuration Changes

- If you modify the configuration structure (`ep_file_annotation.config.yaml`), ensure:
  - Pydantic models are updated accordingly
  - Documentation in `detailed_guides/CONFIG.md` is updated
  - Backward compatibility is maintained or a migration path is provided

## What We're Looking For

Contributions that align with the project's philosophy:

- **Configuration-driven**: Prefer adding configuration options over hardcoded behavior
- **Interface-based**: Extend functionality through interfaces rather than modifying core logic
- **Well-documented**: Code should be self-explanatory with clear documentation
- **Production-ready**: Code should handle edge cases, errors, and scale considerations
- **Backward compatible**: Avoid breaking changes unless absolutely necessary

## Types of Contributions We Welcome

- **Bug fixes**: Resolve issues, fix edge cases, improve error handling
- **Performance improvements**: Optimize queries, caching, or processing logic
- **Documentation**: Improve guides, add examples, clarify confusing sections
- **New configuration options**: Add flexibility through new config parameters
- **New service implementations**: Create alternative implementations of existing interfaces
- **Test coverage**: Add unit tests, integration tests, or test utilities
- **Examples**: Add example configurations or use cases

## Types of Changes Requiring Extra Discussion

These types of changes require significant discussion in the GitHub issue before proceeding:

- Breaking changes to the configuration format
- Changes to the core architecture or interfaces
- New external dependencies
- Changes affecting the data model structure
- Performance changes that trade off memory/CPU/network differently

## Questions or Need Help?

- Create a GitHub issue with your question
- Tag it with the "question" label
- The maintainer will respond as soon as possible

## Code of Conduct

- Be respectful and constructive in all interactions
- Provide thoughtful, actionable feedback during code reviews
- Assume good intentions from all contributors
- Focus on the code and ideas, not the person

## License

By contributing to this project, you agree that your contributions will be licensed under the same license as the project (see LICENSE file).

---

Thank you for contributing to making this project better! 🚀

Return to [Main README](README.md)
