"""Estado de datos: per-store scrape health, resolution coverage and data quality."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.dashboard import data
from src.dashboard.components import category_label
from src.dashboard.formatting import ago, local_datetime

STATUS_LABELS = {
    "ok": "OK",
    "empty": "Sin resultados",
    "anomaly": "Incompleto",
    "error": "Error",
    "running": "En curso",
}

st.title("Estado de datos")
st.markdown(
    "Transparencia sobre de dónde salen los precios: cuándo se leyó cada tienda, cuántos "
    "productos se reconocieron y qué quedó afuera."
)
status = data.status()
st.caption(
    f"Última lectura: {local_datetime(status.get('last_scrape_at'))} "
    f"({ago(status.get('last_scrape_at'))}). Se actualiza dos veces por día."
)

rows = []
stores = sorted(status["stores"].values(), key=lambda s: (bool(s.get("paused")), s["name"]))
for store in stores:
    for category, run in sorted(store["categories"].items()):
        rows.append(
            {
                "Tienda": store["name"],
                "Categoría": category_label(category),
                "Estado": "Pausada"
                if store.get("paused")
                else STATUS_LABELS.get(run["status"], run["status"]),
                "Productos leídos": run["n_found"],
                "Última lectura": ago(run["started_at"]),
                "Último éxito": ago(run["last_ok_at"]),
                "Detalle": store.get("paused") or run["error"] or "",
                "Sitio": store["url"],
            }
        )
st.subheader("Tiendas")
if status.get("stores_paused"):
    st.caption(
        "Las tiendas **pausadas** no se leen por ahora; sus precios anteriores quedan en el "
        "historial pero no cuentan para el precio actual."
    )
st.dataframe(
    pd.DataFrame(rows),
    hide_index=True,
    column_config={"Sitio": st.column_config.LinkColumn(display_text="Abrir ↗")},
)

st.subheader("Reconocimiento de productos")
resolution = status.get("resolution", {})
total = sum(resolution.values()) or 1
cols = st.columns(4)
cols[0].metric("Producto exacto identificado", f"{resolution.get('matched', 0) / total:.0%}")
cols[1].metric(
    "Solo familia (modelo sin agrupar)", f"{resolution.get('family_only', 0) / total:.0%}"
)
cols[2].metric("Descartados (no es del rubro)", resolution.get("rejected", 0))
cols[3].metric("Sin reconocer", resolution.get("unparsed", 0))
st.caption(
    "Un listado se agrupa con los de otras tiendas solo si se identifica el modelo exacto "
    "(marca, línea y especificaciones). Si hay dudas queda como producto propio: es preferible "
    "a mezclar precios de productos distintos."
)

unresolved = data.unresolved()
if not unresolved.empty:
    with st.expander(f"Listados sin reconocer ({len(unresolved)})"):
        st.dataframe(
            unresolved,
            hide_index=True,
            column_config={"url": st.column_config.LinkColumn(display_text="Ver ↗")},
        )

issues = data.quality_issues()
st.subheader("Control de calidad de precios")
if issues.empty:
    st.success("No se detectaron precios dudosos en las ofertas actuales.", icon=":material/check:")
else:
    st.dataframe(
        issues,
        hide_index=True,
        column_config={"url": st.column_config.LinkColumn(display_text="Ver ↗")},
    )
