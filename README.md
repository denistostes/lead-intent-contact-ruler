# Lead Intent & Contact Ruler (Motor de Decisão)

Este repositório contém a refatoração modular de um workflow de otimização de discagem (originalmente desenvolvido em Databricks Notebooks) para scripts SQL e Python de nível produtivo, estruturados para integração em pipelines de dados e ambientes de CI/CD.

## Objetivo
O objetivo central é construir uma estratégia operacional que **maximize o volume de oportunidades esperadas**, combinando quatro pilares fundamentais:
* Sinais de intenção do lead (**Lead Intent**);
* Curvas de exaustão de contato (**Contact Ruler**);
* Clusterização de performance de analistas (**Tierização**);
* Otimização matemática de alocação de headcount (**Programação Linear**).

## Visão Geral da Estratégia

### 1. Intent de Lead
A pipeline consolida eventos de telefonia e do funil do Data Warehouse em uma base canônica única (`base_inteligencia_dialer`), integrando sinais de campanha, performance histórica do analista e propensão de conversão por canal.

### 2. Régua de Contato (Lógica de Exaustão)
O motor de decisão determina o ponto exato onde o esforço de contato começa a perder valor marginal. A exaustão é modelada sob duas óticas complementares:
* **Profundidade de tentativas:** Número de discagens efetuadas (`n_tentativa`);
* **Idade do lead:** Tempo de permanência na base em dias (`dias_vida`).

Para garantir precisão em bases de baixo volume, aplicamos uma **suavização volumétrica via Raiz Quadrada (SQRT)**, evitando que ruídos estatísticos em amostras pequenas distorçam a priorização:
`score_p2c = eficiencia_relativa * SQRT(pool_prospect_pct / 100)`

### 3. Conceito de Elasticidade
A Elasticidade mede a sensibilidade da conversão em relação à qualidade do talento alocado em cada fila:
* Comparamos a performance do grupo de elite (*Top-Half*) versus o grupo em desenvolvimento (*Bottom-Half*);
* Calculamos a distância de performance em pontos percentuais para identificar canais "elásticos" (que dependem de alta senioridade) e "inelásticos" (resilientes);
* **Ação Tática:** Segmentos de alta elasticidade recebem prioridade de alocação dos melhores analistas para **maximizar a conversão em oportunidades**.

### 4. Otimização Prescritiva (PuLP)
Utilizamos o solver **PuLP** para resolver um problema de otimização combinatória. O motor **maximiza o volume total de oportunidades esperadas** respeitando restrições operacionais rígidas:
* **Unicidade:** Cada analista pode ser alocado em no máximo uma campanha por turno;
* **Capacidade:** Respeito estrito aos limites de vagas (*seats*) por fila de discagem;
* **Balanceamento:** Alocação forçada ao limite de `min(analistas disponíveis, capacidade total de vagas)`.

## Estrutura do Repositório
* `01_data_prep.sql`: Normalização de mapeamento de campanhas e criação da base canônica de eventos.
* `02_exhaustion_logic.sql`: Cálculo das curvas de conversão marginal e acumulada com aplicação de suavização SQRT.
* `03_clustering_models.py`: Implementação de K-Means para segmentação de Tiers de campanhas e quadrantes de analistas.
* `04_allocation_solver.py`: Construção da matriz de valor esperado e execução do motor de otimização binária.

## Privacidade e Compliance
Este projeto foi integralmente anonimizado para atender a políticas de confidencialidade e LGPD:
* **Schemas:** Referenciados genericamente como `telephony_system` e `data_warehouse`;
* **Produtos:** Nomeados como `Core_Product_A`, `Core_Product_B`, etc.;
* **Parceiros:** Identificados como `Partner_BPO_1`.

---

**Nota Técnica:** Para reproduzir os resultados, execute a sequência numérica dos scripts (01 a 04), garantindo que os outputs SQL sejam materializados antes da execução dos módulos Python.
