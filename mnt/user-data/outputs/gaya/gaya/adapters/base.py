"""
gaya.adapters.base
------------------
Abstract contract for all Gaya connectors.

Design rules:
    - Adapters are the ONLY place that touch external systems
    - collect() is the single impure entry point
    - Everything it returns (TableStats) is frozen and pure from that point on
    - Adapters must never bleed state into check logic

Connector implementations: postgres.py, snowflake.py (paid), etc.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from gaya.checks.base import TableStats


class DataAdapter(ABC):
    """
    Base class for all connectors.

    Each connector implements collect() to query the source system
    and return a fully-populated, immutable TableStats object.

    Once collect() returns, the adapter's job is done.
    No check function ever calls back into the adapter.
    """

    @abstractmethod
    def collect(self, table: str, layer: str) -> TableStats:
        """
        Query the source system and return aggregated table stats.

        Implementations must:
            - Compute null counts per column
            - Compute distinct counts per column
            - Record min/max for numeric and date columns
            - Return a frozen TableStats — no lazy evaluation

        Implementations must NOT:
            - Log anything
            - Mutate any shared state
            - Raise generic exceptions — wrap driver errors
              in GayaConnectionError or GayaQueryError
        """
        ...

    @abstractmethod
    def test_connection(self) -> bool:
        """
        Verify the connection is reachable.
        Called by `gaya init` and at the start of `gaya run`.
        Returns True on success, raises GayaConnectionError on failure.
        """
        ...


# ---------------------------------------------------------------------------
# Adapter-specific exceptions (impure layer, so errors live here)
# ---------------------------------------------------------------------------

class GayaConnectionError(Exception):
    """Raised when the adapter cannot reach the data source."""
    pass


class GayaQueryError(Exception):
    """Raised when a query fails during stat collection."""
    pass
