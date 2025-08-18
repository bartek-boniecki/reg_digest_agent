"""
Marks 'app' as a Python package so relative imports like '.pipeline' work
everywhere (VS Code, uvicorn, Railway workers, etc.).
"""

# Expose settings at package level for convenience in REPLs
from .settings import settings  # noqa: F401