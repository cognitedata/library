# SDK Monkeypatch Documentation

## Overview

This notebook applies a monkeypatch to the Cognite Python SDK's `OAuthInteractive` credential provider to add account selection support for users with multiple Microsoft accounts.

## Problem Statement

When users have multiple Microsoft accounts cached and not all are recently logged in, Microsoft's streamlined auth flow can fail silently or select the wrong account. This is a corner case affecting a small subset of users, but it prevents them from authenticating properly.

## Solution

We monkeypatch the `_refresh_access_token` method in `cognite.client.credentials.OAuthInteractive` to add `prompt="select_account"` to the `acquire_token_interactive()` call. This forces Microsoft's authentication dialog to show an account picker, allowing users to explicitly select which account to use.

## Implementation Details

### Dependencies

- **cognite-toolkit**: `==0.7.69` (pinned version)
- **cognite-sdk**: `v7.91.1` (via toolkit dependency)

The patch is based on the SDK implementation at:
- GitHub: https://github.com/cognitedata/cognite-sdk-python/blob/cognite-sdk-python-v7.91.1/cognite/client/credentials.py#L683-L696

### Code Location

The monkeypatch is applied in the `app.setup` block at the top of `marimo-tsjm-analysis.py` (lines ~19-65), before any CDF client initialization.

### Technical Approach

1. **Name Mangling**: Python's name mangling converts private attributes like `__app` to `_OAuthInteractive__app`. The patched method uses `getattr()` to access these name-mangled attributes.

2. **Minimal Changes**: The patch only modifies the `acquire_token_interactive()` call to include `prompt="select_account"`. All other logic remains identical to the original implementation.

3. **Preservation**: The original method is stored (though not currently used) for potential future reference or rollback.

### Code Structure

```python
# Store original method
_original_refresh_access_token = OAuthInteractive._refresh_access_token

def _refresh_access_token_patch(self) -> tuple[str, float]:
    # Access name-mangled private attributes
    app = getattr(self, "_OAuthInteractive__app")
    scopes = getattr(self, "_OAuthInteractive__scopes")
    redirect_port = getattr(self, "_OAuthInteractive__redirect_port")
    
    # ... (same logic as original) ...
    
    # Only modification: add prompt parameter
    credentials = app.acquire_token_interactive(
        scopes=scopes, port=redirect_port, prompt="select_account"
    )
    
    # ... (rest of original logic) ...

# Apply patch
OAuthInteractive._refresh_access_token = _refresh_access_token_patch
```

## Risks and Limitations

### ⚠️ Version Dependency

**This patch depends on the internal implementation of the Cognite SDK remaining unchanged.**

- If the SDK updates and changes the `_refresh_access_token` method signature or internal structure, the patch may break.
- The patch is tied to SDK version `v7.91.1` (via `cognite-toolkit==0.7.69`).
- Upgrading `cognite-toolkit` may pull in a newer SDK version that breaks compatibility.

### Breaking Changes to Watch For

1. **Method signature changes**: If `_refresh_access_token()` signature changes
2. **Attribute name changes**: If `__app`, `__scopes`, or `__redirect_port` are renamed
3. **Logic restructuring**: If the token refresh flow is significantly restructured
4. **MSAL API changes**: If `acquire_token_interactive()` API changes

### Testing

When upgrading dependencies, verify:
1. Authentication still works for single-account users
2. Account selection dialog appears for multi-account users
3. Token refresh works correctly after initial authentication

## Future Considerations

### Preferred Solutions (in order of preference)

1. **Upstream Fix**: Request the Cognite SDK team to add `prompt` parameter support to `OAuthInteractive` (ideally as a configurable option)
2. **SDK Fork**: Maintain a fork with the fix (if upstream is slow to respond)
3. **Alternative Auth Flow**: Use a different authentication method that doesn't have this issue
4. **Keep Monkeypatch**: Continue using the monkeypatch if upstream fix is not available

### Monitoring

- Monitor for SDK updates that might break the patch
- Consider adding a test that verifies the patch still works after dependency updates
- Document any issues encountered with newer SDK versions

## References

- [MSAL Python Documentation - acquire_token_interactive](https://msal-python.readthedocs.io/en/latest/#msal.PublicClientApplication.acquire_token_interactive)
- [MSAL Python Documentation - Prompt Parameter](https://msal-python.readthedocs.io/en/latest/#msal.Prompt)
- [Cognite SDK Source Code (v7.91.1)](https://github.com/cognitedata/cognite-sdk-python/blob/cognite-sdk-python-v7.91.1/cognite/client/credentials.py)
