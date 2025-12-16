from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from datetime import datetime
import db_connector

router = APIRouter()

@router.get('/api/memories')
async def api_get_memories(
        month: Optional[str] = None,
        day: Optional[str] = None,
        limit: str = '5'
):
    try:
        current_dt = datetime.now()

        parsed_month = int(month) if month and month.isdigit() else current_dt.month
        parsed_day = int(day) if day and day.isdigit() else None

        limit_val = 5
        if limit and limit.isdigit():
            limit_val = max(1, min(int(limit), 10))

        if not 1 <= parsed_month <= 12:
            raise HTTPException(status_code=400, detail="Invalid month.")
        if parsed_day is not None and not 1 <= parsed_day <= 31:
            raise HTTPException(status_code=400, detail="Invalid day.")

        memories = await db_connector.fetch_memories_from_oracle(
            month=parsed_month, day=parsed_day, limit=limit_val
        )
        return {"memories": memories}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to fetch memories.")