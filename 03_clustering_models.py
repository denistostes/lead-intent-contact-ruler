"""03_clustering_models.py
Clustering modules for campaign tiers and analyst quadrants.
All business labels are anonymized while preserving logic.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict

import pandas as pd
from sklearn.cluster import KMeans

os.environ["OMP_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"


@dataclass
class ClusterConfig:
    n_clusters: int = 4
    random_state: int = 42
    n_init: int = 10


def _ordered_label_map(scores_by_cluster: pd.Series, labels: Dict[int, str]) -> Dict[int, str]:
    ranking = scores_by_cluster.sort_values(ascending=False).index.tolist()
    return {ranking[i]: labels[i] for i in range(len(ranking))}


def build_tier_clusters(df: pd.DataFrame, config: ClusterConfig = ClusterConfig()) -> pd.DataFrame:
    """Replicates planning cluster tier KMeans using score_ponderado."""
    if df.empty or len(df) < config.n_clusters:
        raise ValueError("Insufficient data for tier clustering")

    x = df[["score_ponderado"]]
    model = KMeans(n_clusters=config.n_clusters, random_state=config.random_state, n_init=config.n_init)
    out = df.copy()
    out["cluster_temp_id"] = model.fit_predict(x)

    cmap = _ordered_label_map(
        out.groupby("cluster_temp_id")["score_ponderado"].mean(),
        {
            0: "1. TIER 1 (HIGH_PERFORMANCE)",
            1: "2. TIER 2 (MEDIUM_PERFORMANCE)",
            2: "3. TIER 3 (LOW_PERFORMANCE)",
            3: "4. TIER 4 (VERY_LOW_PERFORMANCE)",
        },
    )
    out["nome_grupo"] = out["cluster_temp_id"].map(cmap)
    return out.drop(columns=["cluster_temp_id"])


def build_analyst_quadrants(df: pd.DataFrame, config: ClusterConfig = ClusterConfig()) -> pd.DataFrame:
    """Replicates analyst KMeans (90d + 30d) preserving notebook decision logic."""
    if df.empty or len(df) < config.n_clusters:
        raise ValueError("Insufficient data for analyst clustering")

    out = df.copy()

    x_90d = out[["score_ponderado_90d"]]
    k90 = KMeans(n_clusters=config.n_clusters, random_state=config.random_state, n_init=config.n_init)
    out["temp_id_90d"] = k90.fit_predict(x_90d)
    map90 = _ordered_label_map(
        out.groupby("temp_id_90d")["score_ponderado_90d"].mean(),
        {
            0: "1. Q1 (HIGH_PERFORMANCE)",
            1: "2. Q2 (MEDIUM_PERFORMANCE)",
            2: "3. Q3 (LOW_PERFORMANCE)",
            3: "4. Q4 (VERY_LOW_PERFORMANCE)",
        },
    )
    out["quadrante_historico_90d"] = out["temp_id_90d"].map(map90)
    out = out.drop(columns=["temp_id_90d"])

    rec = out[out["vol_conectadas_30d"] > 0].copy()
    if len(rec) >= config.n_clusters:
        x_30d = rec[["score_ponderado_30d"]]
        k30 = KMeans(n_clusters=config.n_clusters, random_state=config.random_state, n_init=config.n_init)
        rec["temp_id_30d"] = k30.fit_predict(x_30d)
        map30 = _ordered_label_map(
            rec.groupby("temp_id_30d")["score_ponderado_30d"].mean(),
            {
                0: "1. Q1 (HIGH_PERFORMANCE)",
                1: "2. Q2 (MEDIUM_PERFORMANCE)",
                2: "3. Q3 (LOW_PERFORMANCE)",
                3: "4. Q4 (VERY_LOW_PERFORMANCE)",
            },
        )
        rec["quadrante_recente_30d"] = rec["temp_id_30d"].map(map30)
        out = out.merge(rec[["email", "quadrante_recente_30d"]], on="email", how="left")
    else:
        out["quadrante_recente_30d"] = "Inactive"

    out["quadrante_recente_30d"] = out["quadrante_recente_30d"].fillna("Inactive")
    return out


def build_elasticity_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Computes Tier x Quadrant elasticity metrics from notebook logic."""
    req = {
        "planning_cluster_obt",
        "is_connect",
        "is_opp",
        "id_lead",
        "nome_grupo",
        "quadrante_historico_90d",
        "quadrante_recente_30d",
    }
    missing = req - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns for elasticity matrix: {sorted(missing)}")

    work = df.copy()
    work["quartil_analista"] = work["quadrante_historico_90d"]
    agg = (
        work.groupby(["planning_cluster_obt", "nome_grupo", "quartil_analista"], as_index=False)
        .agg(
            vol_leads=("id_lead", "nunique"),
            vol_conectadas=("is_connect", "sum"),
            vol_opps=("is_opp", "sum"),
        )
    )
    agg["c2o_pct"] = (agg["vol_opps"] * 100.0 / agg["vol_conectadas"].replace(0, pd.NA)).fillna(0).round(2)
    return agg
