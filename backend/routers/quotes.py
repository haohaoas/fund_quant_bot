# backend/routers/quotes.py
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend import portfolio_service as ps

router = APIRouter()


class QuotePayload(BaseModel):
    code: str
    nav: float


@router.post("/api/quotes")
def create_quote(payload: QuotePayload):
    try:
        quote_id = ps.add_quote(code=str(payload.code), nav=float(payload.nav))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"ok": True, "id": quote_id}


@router.get("/api/quotes/latest")
def latest_quote(code: str):
    q = ps.get_latest_quote(code)
    if not q:
        raise HTTPException(status_code=404, detail="no quote found")
    return q