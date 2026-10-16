"""
Which import-calculator data needs a manual re-check (courier tariffs, rules, DNA registry).

    python -m src.crossborder.freshness            # prints a Markdown checklist, or nothing

Used by .github/workflows/freshness.yml, which opens an issue when something is stale.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date

from src.crossborder.config import CrossBorderConfig, load_config
from src.crossborder.verdict import STALE_AFTER_DAYS


@dataclass(frozen=True)
class StaleItem:
    what: str
    checked: date
    url: str
    file: str

    def days(self, today: date) -> int:
        return (today - self.checked).days


def stale_items(
    cfg: CrossBorderConfig, today: date, max_days: int = STALE_AFTER_DAYS
) -> list[StaleItem]:
    items = [
        StaleItem(f"Tarifa de {c.name}", c.verified_at, c.source_url, "configs/couriers.yaml")
        for c in cfg.couriers.values()
    ]
    items += [
        StaleItem(
            f"Reglas «{r.name}»",
            r.verified_at,
            r.source_url("franchise") or "",
            "configs/import_rules.yaml",
        )
        for r in cfg.rules.rule_sets
    ]
    registry = cfg.stores.registry
    items.append(
        StaleItem(
            "Registro de vendedores de EE. UU. (Aduanas)",
            registry.checked_at,
            registry.source_url,
            "configs/us_stores.yaml",
        )
    )
    return [item for item in items if item.days(today) > max_days]


def checklist(items: list[StaleItem], today: date) -> str:
    lines = [
        f"Datos de la calculadora de importación sin revisar hace más de {STALE_AFTER_DAYS} "
        "días. Para cada uno: abrir la fuente, actualizar los números y la fecha de "
        "verificación en el archivo (ver docs/importacion.md, sección Maintenance).",
        "",
    ]
    lines += [
        f"- [ ] {item.what}: verificado el {item.checked:%d/%m/%Y} "
        f"({item.days(today)} días) · [fuente]({item.url}) · `{item.file}`"
        for item in items
    ]
    return "\n".join(lines)


def main() -> int:
    today = date.today()
    items = stale_items(load_config(), today)
    if items:
        print(checklist(items, today))
    return 0


if __name__ == "__main__":
    sys.exit(main())
