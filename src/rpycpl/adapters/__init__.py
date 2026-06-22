"""Coupling adapter interface and concrete REMIND backends."""

from __future__ import annotations

from rpycpl.adapters.base import CouplingAdapter
from rpycpl.adapters.gdx import RemindGdxAdapter
from rpycpl.adapters.iamc import RemindIamcAdapter

__all__ = ["CouplingAdapter", "RemindGdxAdapter", "RemindIamcAdapter"]
