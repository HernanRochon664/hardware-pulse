"""
Courier charges for one shipment: freight by weight band, volume surcharge,
TFSPU, handling, insurance and delivery (tariffs in configs/couriers.yaml).

Flat bands ("1 to 899 g") are matched on the real weight; per-kg bands on the
weight rounded up to the courier's step, and the whole rounded weight is
charged at the band's price (so 5.1 kg can cost less than 5.0 kg, as published).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal

from src.crossborder.config import Courier
from src.crossborder.money import CostLine, cents, kg, pct, usd

DeliveryOption = Literal["pickup", "montevideo", "interior"]

_EPS = 1e-9


def ceil_to_step(weight_kg: float, step_kg: float) -> float:
    """Round a weight up to the courier's step (step 0.1: 1.21 kg -> 1.3 kg), at least one step."""
    steps = math.ceil(round(weight_kg / step_kg, 6))
    return round(max(steps, 1) * step_kg, 3)


@dataclass(frozen=True)
class Freight:
    amount: float
    chargeable_kg: float
    detail: str


def band_freight(courier: Courier, weight_kg: float) -> Freight | None:
    """Freight of a weight; None when it is above the courier's last band."""
    for band in courier.bands:
        if band.flat_usd is not None and weight_kg <= band.up_to_kg + _EPS:
            detail = f"precio fijo hasta {kg(band.up_to_kg)} ({kg(weight_kg)})"
            return Freight(band.flat_usd, weight_kg, detail)
    rounded = ceil_to_step(weight_kg, courier.weight_step_kg)
    for band in courier.bands:
        if band.per_kg_usd is not None and rounded <= band.up_to_kg + _EPS:
            detail = f"{kg(rounded)} × {usd(band.per_kg_usd)}/kg"
            if rounded > weight_kg + _EPS:
                detail += f" (peso real {kg(weight_kg)}, redondeado)"
            return Freight(band.per_kg_usd * rounded, rounded, detail)
    return None


@dataclass(frozen=True)
class CourierCharges:
    lines: tuple[CostLine, ...]
    chargeable_kg: float
    notes: tuple[str, ...]


def courier_charges(
    courier: Courier,
    *,
    real_kg: float,
    box_cm3: float | None,
    invoice_usd: float,
    iva_rate: float,
    insurance: bool = False,
    delivery: DeliveryOption = "pickup",
) -> CourierCharges | None:
    """Every courier line for a shipment, or None if the courier cannot carry it."""
    notes: list[str] = []
    weight = real_kg
    surcharge = 0.0
    surcharge_detail = ""
    volumetric = courier.volumetric
    if volumetric is not None and box_cm3:
        volume_kg = box_cm3 / volumetric.divisor_cm3_per_kg
        if volumetric.surcharge_per_kg_usd is None:
            weight = max(real_kg, volume_kg)
        else:
            excess = volume_kg - volumetric.free_ratio * real_kg
            if excess > _EPS:
                extra = ceil_to_step(excess, courier.weight_step_kg)
                surcharge = extra * volumetric.surcharge_per_kg_usd
                surcharge_detail = (
                    f"{kg(extra)} de volumen por encima de {volumetric.free_ratio:g}× el peso "
                    f"real × {usd(volumetric.surcharge_per_kg_usd)}/kg"
                )
        if not volumetric.divisor_published:
            notes.append(
                f"{courier.name} no publica su divisor volumétrico: se asume "
                f"{volumetric.divisor_cm3_per_kg:g} cm³/kg."
            )

    freight = band_freight(courier, weight)
    if freight is None:
        return None
    if not courier.weight_step_published and freight.chargeable_kg >= 1:
        notes.append(
            f"{courier.name} no publica cómo redondea el peso: se asume cada "
            f"{kg(courier.weight_step_kg)}."
        )

    lines = [
        CostLine(
            "freight",
            f"Flete {courier.name}",
            cents(freight.amount),
            freight.detail,
            courier.source_url,
            "courier",
        )
    ]
    if surcharge:
        lines.append(
            CostLine(
                "volume",
                "Recargo por volumen",
                cents(surcharge),
                surcharge_detail,
                courier.source_url,
                "courier",
            )
        )

    tfspu_base = freight.amount + surcharge
    tfspu_parts = "flete"
    if courier.handling_usd:
        handling = courier.handling_usd * (1 + iva_rate if courier.handling_iva else 1)
        detail = f"{usd(courier.handling_usd)} por envío"
        if courier.handling_iva:
            detail += f" + IVA {pct(iva_rate)}"
        lines.append(
            CostLine("handling", "Gestión del courier", cents(handling), detail, None, "courier")
        )
        if courier.tfspu_on_handling:
            tfspu_base += courier.handling_usd
            tfspu_parts = "flete y gestión"
    if courier.tfspu_rate:
        lines.append(
            CostLine(
                "tfspu",
                "Tasa postal TFSPU (URSEC)",
                cents(courier.tfspu_rate * tfspu_base),
                f"{pct(courier.tfspu_rate)} de {usd(tfspu_base)} ({tfspu_parts}, sin IVA)",
                "https://www.impo.com.uy/bases/leyes/19009-2012",
                "courier",
            )
        )

    if insurance:
        if courier.insurance is None:
            notes.append(f"{courier.name} no publica el costo del seguro: no está incluido.")
        else:
            premium = max(courier.insurance.rate * invoice_usd, courier.insurance.min_usd)
            lines.append(
                CostLine(
                    "insurance",
                    "Seguro",
                    cents(premium),
                    f"{pct(courier.insurance.rate)} del valor (mínimo "
                    f"{usd(courier.insurance.min_usd)})",
                    courier.source_url,
                    "courier",
                )
            )

    if delivery != "pickup":
        line = _delivery_line(courier, delivery, freight.chargeable_kg, iva_rate)
        if line is None:
            where = "a domicilio" if delivery == "montevideo" else "al interior"
            notes.append(f"{courier.name} no publica el costo de envío {where}: no está incluido.")
        else:
            lines.append(line)

    return CourierCharges(tuple(lines), freight.chargeable_kg, tuple(notes))


def _delivery_line(
    courier: Courier, delivery: DeliveryOption, chargeable_kg: float, iva_rate: float
) -> CostLine | None:
    tariff = courier.delivery
    if tariff is None:
        return None
    if delivery == "montevideo":
        if tariff.montevideo_usd is None:
            return None
        base, label = tariff.montevideo_usd, "Entrega a domicilio en Montevideo"
        detail = f"{usd(base)}"
    else:
        if tariff.interior_usd is None:
            return None
        extra_kg = max(0, math.ceil(round(chargeable_kg, 6)) - 1)
        base = tariff.interior_usd + extra_kg * tariff.interior_per_extra_kg_usd
        label = "Envío al interior"
        detail = f"{usd(tariff.interior_usd)}"
        if extra_kg and tariff.interior_per_extra_kg_usd:
            detail += f" + {extra_kg} kg × {usd(tariff.interior_per_extra_kg_usd)}"
    if tariff.iva:
        detail += f" + IVA {pct(iva_rate)}"
        base *= 1 + iva_rate
    return CostLine("delivery", label, cents(base), detail, courier.source_url, "courier")
