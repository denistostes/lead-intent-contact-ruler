# Lead Intent & Contact Ruler (Motor de Otimização de Discagem)

> **Nota:** Este projeto está documentado em inglês para refletir os padrões técnicos de Engenharia de Dados e Pesquisa Operacional. 
> 
> **Resumo:** Este repositório contém um motor de decisão para operações de Inside Sales. Ele automatiza a identificação do ponto de saturação de leads (Exhaustion), segmenta a performance da equipe via Machine Learning (Elasticidade) e utiliza Programação Linear para alocar o time nos leads com maior probabilidade de conversão.

# Lead Intent & Contact Ruler

This repository contains a production-grade refactor of a notebook-driven dialing optimization workflow into modular SQL/Python assets for GitHub and CI/CD usage.

## Goal

Build an operational strategy that maximizes expected opportunities by combining:
- lead intent signals;
- contact-exhaustion curves;
- analyst performance clustering;
- mathematical allocation optimization.

## Strategy Overview

### 1) Lead Intent
The pipeline consolidates telephony events and warehouse funnel events into a single canonical base (`base_inteligencia_dialer`) with campaign, analyst, and conversion signals.

### 2) Contact Ruler
The Contact Ruler is a decision engine that determines where contact effort starts losing marginal value.

Two complementary exhaustion lenses are modeled:
- **Prospect attempt depth** (`n_tentativa`);
- **Lead age in days** (`dias_vida`).

Both engines compute marginal and cumulative conversion curves and preserve the same notebook formulas, including **SQRT volumetric smoothing**:

`score_p2c = eficiencia_relativa * SQRT(pool_prospect_pct / 100)`

This avoids over-weighting sparse-volume spikes while keeping high-signal zones prioritized.

### 3) Elasticity Concept
Elasticity measures conversion sensitivity to talent quality allocation:
- compare top-half analyst performance (Q1+Q2) vs bottom-half (Q3+Q4);
- compute performance distance in percentage points;
- compare each segment against global baseline.

Operationally, high-elasticity segments benefit more from stronger analyst allocation and should receive priority in staffing decisions.

### 4) Optimization (PuLP)
The solver maximizes expected opportunities with binary decision variables and strict constraints:
- each analyst can be assigned to at most one campaign;
- each campaign has seat-capacity limits;
- total assignments are forced to `min(available analysts, total seats)`.

This produces an allocation that is mathematically consistent and operations-ready.

## Repository Structure

- `01_data_prep.sql`: normalized campaign mapping, user/BPO anonymization, and canonical event base.
- `02_exhaustion_logic.sql`: contact ruler logic for attempt-depth and age-based exhaustion, with SQRT smoothing preserved.
- `03_clustering_models.py`: KMeans modules for campaign-tier and analyst-quadrant segmentation.
- `04_allocation_solver.py`: PuLP expected-value matrix and linear/binary optimization engine.

## Privacy and Anonymization

This refactor applies mandatory anonymization:
- schemas: `telephony_system` and `data_warehouse`;
- product naming: `Core_Product_A`, `Core_Product_B`, `Core_Product_C`, etc.;
- BPO partner labels: `Partner_BPO_1`.

## How To Run

1. Execute `01_data_prep.sql`.
2. Execute `02_exhaustion_logic.sql`.
3. Materialize clustering inputs and run `03_clustering_models.py`.
4. Feed the golden-set dataframe into `04_allocation_solver.py`.

## Operational Notes

- Keep date filters in config views for easy period refresh.
- Re-train clustering periodically (for example weekly) to capture drift.
- Re-evaluate elasticity baseline after major campaign-mix shifts.
- Treat solver outputs as decision support and combine with workforce constraints (schedules, absenteeism, training stage).
