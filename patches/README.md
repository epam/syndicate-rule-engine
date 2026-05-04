

- a patch is a docker executable that will be executed as K8S job
- patch should not break the data if executed more than once
- patch executable has access to minio vault and mongo credentials
- patch should wait till the patched service is started


## Patch Structure

Each patch version should be organized in a directory named after the version (e.g., `5.17.0/`).

### Required Files

1. **`main.py`** - Entry point for the patch execution
2. **`Dockerfile`** - Docker image definition for the patch
3. **Patch modules** - One or more Python files implementing individual patches (e.g., `patch_*.py`)

### Patch Module Structure

Each patch module must implement a class that inherits from `BasePatch`:

```python
"""
Patch description explaining what the patch does.

The patch should be idempotent — running it more than once is safe.
"""

import sys

from helpers.log_helper import get_logger
from models.your_model import YourModel
from common.base import BasePatch

_LOG = get_logger(__name__)


class PatchYourFeature(BasePatch):
    """Patch for your feature description."""

    @property
    def name(self) -> str:
        """Return the name of the patch."""
        return "your_feature_name"

    def _execute(self) -> int:
        """Execute the patch logic. Returns 0 on success, 1 on failure."""
        try:
            _LOG.info("Starting patch execution")
            
            if self.dry_run:
                _LOG.info("DRY RUN mode: no changes will be written")

            # Your patch logic here
            # Always check self.dry_run before writing changes:
            if not self.dry_run:
                # Perform actual updates
                pass
            else:
                # Log what would be done
                _LOG.info("DRY RUN: Would perform update...")
            
            return 0
        except Exception:
            _LOG.exception("Unexpected exception")
            return 1


def main() -> int:
    """Main function for standalone execution."""
    patch = PatchYourFeature()
    return patch.run()


if __name__ == "__main__":
    sys.exit(main())
```

### Main Entry Point (`main.py`)

The `main.py` file should use the common patch infrastructure by importing `main` from `common` and passing the list of patches:

```python
from common import main as patch_main

from patch_your_feature import PatchYourFeature

if __name__ == "__main__":
    patches = [
        PatchYourFeature(),
        # Add more patches here
    ]
    patch_main(patches)
```

The `main()` function from `common` handles:
- Command line argument parsing (`--dry-run`, `--isolated`)
- Logging configuration
- Patch execution orchestration

### Patch Requirements

1. **Inherit from `BasePatch`**: All patches must inherit from `common.base.BasePatch`
2. **Implement `name` property**: Return a unique string identifier for the patch
3. **Implement `_execute()` method**: Contains the actual patch logic
   - Must return `0` on success, `1` on failure
   - Should handle exceptions and log them appropriately
4. **Support `dry_run` mode**: Check `self.dry_run` before making any changes
   - When `dry_run=True`, log what would be done instead of making changes
5. **Idempotency**: Patches must be safe to run multiple times
6. **Logging**: Use proper logging throughout the patch execution
7. **Standalone execution**: Include a `main()` function for standalone testing

### Patch Flags

Patches support two execution modes:

- **`--dry-run`**: Runs the patch without writing any changes. Useful for testing and validation.
- **`--isolated`**: Runs the patch in isolated mode for CI. In this mode, the patch logic is skipped (returns 0 immediately).

### Dockerfile Structure

The Dockerfile should:
- Use the base Python image
- Copy necessary dependencies (`pyproject.toml`, `uv.lock`)
- Copy patch files
- Set up the working directory
- Use `main.py` as the entry point

Example:
```dockerfile
FROM public.ecr.aws/docker/library/python:3.10 as builder
# ... build steps ...

FROM public.ecr.aws/docker/library/python:3.10-slim
# ... runtime setup ...
ENTRYPOINT ["python", "/src/main.py"]
```

## Building

Assuming that your working directory is `patches/`


Export the necessary patch version to env `PATCH_VERION`:

```bash
export PATCH_VERSION=<specify version here>
```


Build for arm64:
```bash
podman build --platform linux/arm64 -t public.ecr.aws/x4s4z8e1/syndicate/patches:rule-engine-"$PATCH_VERSION"-arm64 -f ./"$PATCH_VERSION/Dockerfile" ./"$PATCH_VERSION"
```

Build for amd64:
```bash
podman build --platform linux/amd64 -t public.ecr.aws/x4s4z8e1/syndicate/patches:rule-engine-"$PATCH_VERSION"-amd64 -f ./"$PATCH_VERSION/Dockerfile" ./"$PATCH_VERSION"
```

Push both versions:

```bash
podman push public.ecr.aws/x4s4z8e1/syndicate/patches:rule-engine-"$PATCH_VERSION"-arm64
podman push public.ecr.aws/x4s4z8e1/syndicate/patches:rule-engine-"$PATCH_VERSION"-amd64
```

Create image manifest

```bash
podman manifest rm public.ecr.aws/x4s4z8e1/syndicate/patches:rule-engine-"$PATCH_VERSION" || true
podman manifest create public.ecr.aws/x4s4z8e1/syndicate/patches:rule-engine-"$PATCH_VERSION" public.ecr.aws/x4s4z8e1/syndicate/patches:rule-engine-"$PATCH_VERSION"-arm64 public.ecr.aws/x4s4z8e1/syndicate/patches:rule-engine-"$PATCH_VERSION"-amd64
podman manifest annotate public.ecr.aws/x4s4z8e1/syndicate/patches:rule-engine-"$PATCH_VERSION" public.ecr.aws/x4s4z8e1/syndicate/patches:rule-engine-"$PATCH_VERSION"-arm64 --arch arm64
podman manifest annotate public.ecr.aws/x4s4z8e1/syndicate/patches:rule-engine-"$PATCH_VERSION" public.ecr.aws/x4s4z8e1/syndicate/patches:rule-engine-"$PATCH_VERSION"-amd64 --arch amd64
```

Push manifest
```bash
podman manifest push public.ecr.aws/x4s4z8e1/syndicate/patches:rule-engine-"$PATCH_VERSION"
```