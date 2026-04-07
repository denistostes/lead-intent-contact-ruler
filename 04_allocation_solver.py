"""04_allocation_solver.py
PuLP allocation solver for Lead Intent & Contact Ruler.
Preserves objective function and constraints from notebook.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

import numpy as np
import pandas as pd
import pulp


@dataclass
class CampaignParams:
    hcs_totais: int
    topo_leads: float
    spin_medio: float
    hit_rate: float
    pct_volume_sse: float | None = None


def build_default_inputs() -> Tuple[Dict[str, int], float, float, float, float]:
    params_price = CampaignParams(hcs_totais=20, topo_leads=40086, spin_medio=11.9, hit_rate=0.075, pct_volume_sse=0.55)
    params_a = CampaignParams(hcs_totais=16, topo_leads=54434, spin_medio=8.1, hit_rate=0.089)
    params_c = CampaignParams(hcs_totais=7, topo_leads=7269, spin_medio=11.1, hit_rate=0.09)

    vagas_price_sse = round(params_price.hcs_totais * float(params_price.pct_volume_sse))
    vagas_price_ssi = params_price.hcs_totais - vagas_price_sse

    vagas = {
        "CORE_PRODUCT_B_SSE": vagas_price_sse,
        "CORE_PRODUCT_B_SSI": vagas_price_ssi,
        "CORE_PRODUCT_A_SS": params_a.hcs_totais,
        "CORE_PRODUCT_C_SS": params_c.hcs_totais,
    }

    con_hc_price = (params_price.topo_leads * params_price.spin_medio * params_price.hit_rate) / params_price.hcs_totais
    con_hc_a = (params_a.topo_leads * params_a.spin_medio * params_a.hit_rate) / params_a.hcs_totais
    con_hc_c = (params_c.topo_leads * params_c.spin_medio * params_c.hit_rate) / params_c.hcs_totais

    return vagas, con_hc_price, con_hc_a, con_hc_c, 0.0095


def solve_allocation(
    df: pd.DataFrame,
    vagas: Dict[str, int],
    con_hc_price: float,
    con_hc_a: float,
    con_hc_c: float,
    adicional_uplift_pp: float,
    peso_90d: float = 0.60,
    peso_30d: float = 0.40,
    fator_penalidade: float = 0.85,
) -> tuple[pd.DataFrame, pulp.LpProblem]:
    if df.empty:
        raise ValueError("Input dataframe is empty")

    out = df.copy().fillna(0)

    cols_c2o = ["c2o_price_90d", "c2o_price_30d", "c2o_indicaai_90d", "c2o_indicaai_30d", "c2o_owner_90d", "c2o_owner_30d"]
    for c in cols_c2o:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0) / 100.0

    out["c2o_pond_price"] = (out["c2o_price_90d"] * peso_90d) + (out["c2o_price_30d"] * peso_30d)
    out["c2o_pond_indicaai"] = (out["c2o_indicaai_90d"] * peso_90d) + (out["c2o_indicaai_30d"] * peso_30d)
    out["c2o_pond_owner"] = (out["c2o_owner_90d"] * peso_90d) + (out["c2o_owner_30d"] * peso_30d)

    out["is_eligible_sse"] = (
        out["quadrante_historico_90d"].astype(str).str.contains("Q1|Q2", case=False, na=False)
        | out["quadrante_recente_30d"].astype(str).str.contains("Q1|Q2", case=False, na=False)
    )

    out["VE_CORE_PRODUCT_B_SSE"] = np.where(
        out["is_eligible_sse"],
        (out["c2o_pond_price"] + adicional_uplift_pp) * con_hc_price,
        out["c2o_pond_price"] * con_hc_price,
    ) * fator_penalidade

    out["VE_CORE_PRODUCT_B_SSI"] = (out["c2o_pond_price"] * con_hc_price) * fator_penalidade
    out["VE_CORE_PRODUCT_A_SS"] = (out["c2o_pond_indicaai"] * con_hc_a) * fator_penalidade
    out["VE_CORE_PRODUCT_C_SS"] = (out["c2o_pond_owner"] * con_hc_c) * fator_penalidade

    campanhas = list(vagas.keys())
    prob = pulp.LpProblem("Optimization_Golden_Set", pulp.LpMaximize)
    idx = out.index.tolist()
    x = pulp.LpVariable.dicts("allocation", ((i, j) for i in idx for j in campanhas), cat="Binary")

    prob += pulp.lpSum(x[i, j] * out.loc[i, f"VE_{j}"] for i in idx for j in campanhas)

    for i in idx:
        prob += pulp.lpSum(x[i, j] for j in campanhas) <= 1

    for j in campanhas:
        prob += pulp.lpSum(x[i, j] for i in idx) <= vagas[j]

    total_alocacoes_desejadas = min(len(idx), sum(vagas.values()))
    prob += pulp.lpSum(x[i, j] for i in idx for j in campanhas) == total_alocacoes_desejadas

    prob.solve(pulp.PULP_CBC_CMD(msg=0))

    results = []
    for i in idx:
        allocated = "Not_Allocated"
        opps_projected = 0.0
        for j in campanhas:
            if pulp.value(x[i, j]) == 1:
                allocated = j
                opps_projected = float(out.loc[i, f"VE_{j}"])
                break
        results.append({"email": out.loc[i, "email"], "allocated_campaign": allocated, "opps_expected": opps_projected})

    return pd.DataFrame(results), prob
