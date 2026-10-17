"""Model card of the buy/wait signal: what it predicts and how well it backtests."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.dashboard import data
from src.dashboard.formatting import local_datetime, pct

st.title("Señal comprar / esperar")
st.markdown(
    "Para cada producto con historial suficiente, un modelo estima la **probabilidad de que el "
    "precio más bajo baje al menos 5% en los próximos 14 días**. Si es alta conviene esperar; "
    "si es baja, el precio actual es razonable. La señal solo se publica cuando el modelo le "
    "gana a una regla simple en un backtest honesto (sin mirar el futuro)."
)

model = data.latest_model()
if model is None:
    st.info(
        "Todavía no se entrenó ningún modelo: se necesita más historial. Mientras tanto los "
        "productos muestran solo precios.",
        icon=":material/hourglass_empty:",
    )
    st.stop()

metrics = model["metrics"]
if not model["is_active"]:
    st.warning(
        "**Señal no publicada.** El último modelo no superó al baseline en el backtest "
        f"({metrics.get('gate_reason', 'sin detalle')}), así que los productos no muestran "
        "recomendación. Abajo están sus resultados, a modo de transparencia.",
        icon=":material/block:",
    )
st.caption(f"Versión {model['version']} · entrenado {local_datetime(model['trained_at'])}")
pr_auc = metrics.get("pr_auc") or 0.0
baseline_pr_auc = metrics.get("baseline_pr_auc") or 0.0
cols = st.columns(4)
cols[0].metric(
    "PR-AUC (modelo)",
    f"{pr_auc:.2f}",
    delta=f"{pr_auc - baseline_pr_auc:+.2f} vs baseline",
)
cols[1].metric("Brier skill vs baseline", f"{metrics.get('brier_skill') or 0.0:+.2f}")
cols[2].metric("Tasa base de bajas", pct(metrics.get("base_rate"), signed=False))
cols[3].metric("Ejemplos evaluados", f"{metrics.get('n_test', 0):,}".replace(",", "."))

st.subheader("Qué mira el modelo")
FEATURE_LABELS = {
    "ret_1": "Variación 1 día",
    "ret_7": "Variación 7 días",
    "ret_14": "Variación 14 días",
    "ret_30": "Variación 30 días",
    "dist_min_30": "Distancia al mínimo de 30 días",
    "dist_med_30": "Distancia a la mediana de 30 días",
    "dist_max_30": "Distancia al máximo de 30 días",
    "dist_min_90": "Distancia al mínimo de 90 días",
    "dist_med_90": "Distancia a la mediana de 90 días",
    "dist_max_90": "Distancia al máximo de 90 días",
    "vol_30": "Volatilidad 30 días",
    "days_since_change": "Días desde el último cambio",
    "n_offers": "Cantidad de ofertas",
    "n_stores": "Cantidad de tiendas",
    "family_pos": "Precio frente a la mediana de su familia",
    "family_mom_7": "Tendencia de la familia (7 días)",
    "age_days": "Antigüedad del producto",
    "near_black_friday": "Cerca de Black Friday",
    "cat_gpu": "Categoría: tarjeta de video",
    "cat_cpu": "Categoría: procesador",
    "cat_ram": "Categoría: memoria RAM",
    "cat_ssd": "Categoría: disco SSD",
    "cat_motherboard": "Categoría: motherboard",
    "cat_cooler": "Categoría: cooler de CPU",
    "cat_psu": "Categoría: fuente de poder",
    "cat_case": "Categoría: gabinete",
    "cat_monitor": "Categoría: monitor",
}
features = model["params"]["features"]
coefs = pd.DataFrame(
    {"Variable": [FEATURE_LABELS.get(f, f) for f in features], "Peso": model["params"]["coef"]}
).sort_values(by=["Peso"], key=abs, ascending=False)
st.dataframe(
    coefs,
    hide_index=True,
    column_config={
        "Peso": st.column_config.NumberColumn(
            format="%+.3f", help="Positivo: aumenta la probabilidad de baja"
        ),
    },
)

if metrics.get("calibration"):
    st.subheader("Calibración")
    calib = pd.DataFrame(metrics["calibration"])
    st.dataframe(
        calib.rename(
            columns={
                "bin": "Probabilidad predicha",
                "predicted": "Media predicha",
                "observed": "Frecuencia observada",
                "n": "Ejemplos",
            }
        ),
        hide_index=True,
    )
st.caption(
    f"Backtest walk-forward por fecha con purga de {model['params'].get('horizon_days', 14)} días. "
    f"Ahorro medio al seguir la señal: {pct(metrics.get('avg_saving'), signed=False)}."
)
