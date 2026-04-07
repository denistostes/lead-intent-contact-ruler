# Intent de Lead & Régua de Contato (Motor de Decisão)

Este repositório contém a refatoração modular de um workflow de otimização de discagem (originalmente em notebooks) para scripts SQL e Python de nível produtivo, prontos para CI/CD.

## Objetivo
Construir uma estratégia operacional que maximize o volume de oportunidades esperadas, combinando:
* Sinais de intenção do lead (Lead Intent);
* Curvas de exaustão de contato;
* Clusterização de performance de analistas;
* Otimização matemática de alocação de headcount.

## Visão Geral da Estratégia

### 1. Intent de Lead
A pipeline consolida eventos de telefonia e do funil do Data Warehouse em uma base canônica (`base_inteligencia_dialer`), integrando sinais de campanha, analista e conversão.

### 2. Régua de Contato (Exhaustion Logic)
O motor de decisão determina o ponto onde o esforço de contato perde valor marginal. Modelamos a exaustão sob duas óticas:
* **Profundidade de tentativas** (n_tentativa);
* **Idade do lead em dias** (dias_vida).

Utilizamos a **suavização volumétrica SQRT** (Raiz Quadrada) para evitar distorções em bases de baixo volume, garantindo que zonas de alto sinal sejam priorizadas:
`score_p2c = eficiencia_relativa * SQRT(pool_prospect_pct / 100)`

### 3. Conceito de Elasticidade
A Elasticidade mede a sensibilidade da conversão em relação à qualidade do analista alocado:
* Comparamos a performance do *Top-Half* (Q1+Q2) vs *Bottom-Half* (Q3+Q4);
* Calculamos a distância de performance em pontos percentuais;
* Operacionalmente, segmentos de **Alta Elasticidade** recebem prioridade de alocação dos melhores talentos para maximizar o ROI.

### 4. Otimização Prescritiva (PuLP)
O solver maximiza as oportunidades esperadas respeitando restrições rígidas:
* Cada analista é alocado em no máximo uma campanha;
* Respeito aos limites de capacidade (vagas) por fila;
* Alocação forçada ao limite de `min(analistas disponíveis, vagas totais)`.

## Estrutura do Repositório
* `01_data_prep.sql`: Mapeamento de campanhas, anonimização de BPOs e base canônica.
* `02_exhaustion_logic.sql`: Lógica da Régua de Contato com suavização SQRT.
* `03_clustering_models.py`: Módulos K-Means para segmentação de Tiers e Quadrantes.
* `04_allocation_solver.py`: Matriz de valor esperado e motor de otimização binária.

---
**Nota de Compliance:** Todos os dados, schemas e nomes de parceiros foram anonimizados para preservar a confidencialidade do projeto original.
