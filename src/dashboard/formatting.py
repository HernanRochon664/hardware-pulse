"""Uruguayan number/date formatting and the USD/UYU display preference."""

from __future__ import annotations

from datetime import UTC, datetime

import streamlit as st

from src.settings import LOCAL_TZ, from_iso

CURRENCIES = ("USD", "UYU")


def currency() -> str:
    return st.session_state.get("currency", "USD")


def _group(value: float, decimals: int) -> str:
    text = f"{value:,.{decimals}f}"  # 1,049.50
    return text.replace(",", "§").replace(".", ",").replace("§", ".")  # 1.049,50


def money(usd: float | None, rate: float | None = None, *, cur: str | None = None) -> str:
    """'U$S 1.049' or '$ 42.192' (pesos) following the display preference."""
    if usd is None or usd != usd:  # None or NaN
        return "—"
    cur = cur or currency()
    if cur == "UYU" and rate:
        return f"$ {_group(usd * rate, 0)}"
    decimals = 0 if abs(usd - round(usd)) < 0.005 else 2
    return f"U$S {_group(usd, decimals)}"


def pct(value: float | None, *, signed: bool = True) -> str:
    if value is None or value != value:
        return "—"
    text = _group(abs(value) * 100, 1).rstrip("0").rstrip(",")
    sign = ("+" if value > 0 else "−" if value < 0 else "") if signed else ""
    return f"{sign}{text}%"


def ago(iso: str | None, now: datetime | None = None) -> str:
    if not iso:
        return "nunca"
    delta = (now or datetime.now(UTC)) - from_iso(iso)
    minutes = int(delta.total_seconds() // 60)
    if minutes < 60:
        return f"hace {max(minutes, 1)} min"
    hours = minutes // 60
    if hours < 48:
        return f"hace {hours} h"
    return f"hace {hours // 24} días"


def local_datetime(iso: str | None) -> str:
    if not iso:
        return "—"
    return from_iso(iso).astimezone(LOCAL_TZ).strftime("%d/%m/%Y %H:%M")


def short_date(day: str | None) -> str:
    if not day:
        return "—"
    return datetime.fromisoformat(day).strftime("%d/%m/%Y")
