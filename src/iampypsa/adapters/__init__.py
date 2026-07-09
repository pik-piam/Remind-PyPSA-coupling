"""Coupling adapter interface and IAM-specific backends."""

from __future__ import annotations

from iampypsa.adapters.base import CouplingAdapter
from iampypsa.adapters.gdx import RemindGdxAdapter
from iampypsa.adapters.iamc import RemindIamcAdapter

__all__ = ["CouplingAdapter", "RemindGdxAdapter", "RemindIamcAdapter"]
