"""Helper utilities for working with secret environment variables."""

from __future__ import annotations

from pydantic import SecretStr


def secret_value(value: SecretStr | str | None) -> str | None:
    """Return the plaintext secret stripped of whitespace."""
    if value is None:
        return None
    if isinstance(value, SecretStr):
        raw = value.get_secret_value()
    else:
        raw = value
    stripped = raw.strip()
    return stripped or None
