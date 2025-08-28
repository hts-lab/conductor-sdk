# conductor_sdk/__init__.py
# Expose a single tiny handle for users
from .context import ctx  # noqa: F401

# New: API client conveniences
from .client import ConductorClient, Step, submit_workflow  # noqa: F401
