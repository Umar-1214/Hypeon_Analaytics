"""
Engine run metadata store. Persisted in DB (engine_run_metadata table).
Returns EngineRunMetadata dataclass for API compatibility.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from sqlmodel import select

from packages.governance.src.versions import MTA_VERSION, MMM_VERSION, DATA_SNAPSHOT_ID
from packages.shared.src.db import get_session
from packages.shared.src.models import EngineRunMetadataRecord

_MAX_RECENT_RUNS = 100


@dataclass
class EngineRunMetadata:
    """Metadata for one engine run (returned by get_latest_run / get_recent_runs)."""

    run_id: str
    timestamp: datetime
    mta_version: str
    mmm_version: str
    data_snapshot_id: str


def _record_to_meta(r: EngineRunMetadataRecord) -> EngineRunMetadata:
    return EngineRunMetadata(
        run_id=r.run_id,
        timestamp=r.timestamp,
        mta_version=r.mta_version or "",
        mmm_version=r.mmm_version or "",
        data_snapshot_id=r.data_snapshot_id or "",
    )


def record_run(
    run_id: str,
    mta_version: Optional[str] = None,
    mmm_version: Optional[str] = None,
    data_snapshot_id: Optional[str] = None,
    timestamp: Optional[datetime] = None,
) -> None:
    """
    Record an engine run. Persists to engine_run_metadata table.
    """
    with get_session() as session:
        session.add(
            EngineRunMetadataRecord(
                run_id=run_id,
                timestamp=timestamp or datetime.utcnow(),
                mta_version=mta_version or MTA_VERSION,
                mmm_version=mmm_version or MMM_VERSION,
                data_snapshot_id=data_snapshot_id or DATA_SNAPSHOT_ID,
            )
        )
        session.commit()


def get_recent_runs() -> List[EngineRunMetadata]:
    """Return the most recent runs (up to 100)."""
    with get_session() as session:
        stmt = (
            select(EngineRunMetadataRecord)
            .order_by(EngineRunMetadataRecord.timestamp.desc())
            .limit(_MAX_RECENT_RUNS)
        )
        rows = list(session.exec(stmt).all())
        return [_record_to_meta(r) for r in rows]


def get_latest_run() -> Optional[EngineRunMetadata]:
    """Return the most recent run, or None."""
    with get_session() as session:
        stmt = (
            select(EngineRunMetadataRecord)
            .order_by(EngineRunMetadataRecord.timestamp.desc())
            .limit(1)
        )
        row = session.exec(stmt).first()
        if row is None:
            return None
        return _record_to_meta(row)
