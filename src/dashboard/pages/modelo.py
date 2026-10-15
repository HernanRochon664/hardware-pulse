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

model = data.active_model()
if model is None:
    st.info(
        "Todavía no hay un modelo publicado: se necesita más historial limpio o el modelo no "
        "superó al baseline. Mientras tanto los productos muestran solo precios.",
        icon=":material/hourglass_empty:",
    )
    st.stop()

metrics = model["metrics"]
st.caption(f"Versión {model['version']} · entrenado {local_datetime(model['trained_at'])}")
cols = st.columns(4)
cols[0].metric(
    "PR-AUC (modelo)",
    f"{metrics.get('pr_auc', 0):.2f}",
    delta=f"{metrics.get('pr_auc', 0) - metrics.get('baseline_pr_auc', 0):+.2f} vs baseline",
)
cols[1].metric("Brier skill vs baseline", f"{metrics.get('brier_skill', 0):+.2f}")
cols[2].metric("Tasa base de bajas", pct(metrics.get("base_rate"), signed=False))
cols[3].metric("Ejemplos evaluados", f"{metrics.get('n_test', 0):,}".replace(",", "."))

st.subheader("Qué mira el modelo")
coefs = pd.DataFrame(
    {"Variable": model["params"]["features"], "Peso": model["params"]["coef"]}
).sort_values("Peso", key=abs, ascending=False)
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
