## 🎯 **Objetivo Principal**
Modelagem dimensional em Star Schema para análise estratégica de **indicadores de saúde materno-infantil** com agregações otimizadas para BI.

---

## 🏗️ **Estrutura do Star Schema**

### ⭐ **Tabela Fato Principal:**
**`gold_fato_saude_mensal_cnes`**
- **Granularidade:** Mensal por estabelecimento (CNES) e município
- **Chaves Dimensionais:** `sk_tempo`, `sk_cnes`, `sk_municipio`
- **Métricas:** 12 indicadores estratégicos de saúde

### 📐 **Tabelas de Dimensão:**
| Dimensão | Descrição | Chave |
|:---|:---|:---|
| **⏰ dim_tempo** | Hierarquia temporal (ano → mês) | sk_tempo |
| **🏥 dim_estabelecimentos** | Dados das unidades de saúde | sk_cnes |
| **🗺️ dim_municipios** | Dados geográficos municipais | sk_municipio |

---

## 📈 **Indicadores Calculados**

### 👶 **Indicadores de Nascimento:**
- `total_nascidos_vivos` - Volume de nascimentos
- `perc_prenatal_7_ou_mais_consultas` - % com pré-natal adequado
- `perc_baixo_peso` - % com <2500g
- `perc_partos_cesarea` - % de cesarianas
- `perc_maes_adolescentes` - % mães <20 anos

### ⚠️ **Indicadores de Mortalidade:**
- `total_obitos_infantis` - Óbitos <1 ano
- `taxa_mortalidade_infantil` - por 1000 nascidos
- `total_obitos_neonatais` - Óbitos <28 dias
- `taxa_mortalidade_neonatal` - por 1000 nascidos  
- `total_obitos_maternos` - Óbitos maternos
- `taxa_mortalidade_materna` - por 100.000 nascidos

---

## 🔍 **Dimensões de Análise**

### ⏰ **Temporal:**
- Agregação mensal (ano_mes)
- Análise de tendências
- Sazonalidade

### 🏥 **Por Estabelecimento:**
- Comparativo entre unidades
- Desempenho por CNES
- Benchmarking

### 🗺️ **Geográfica:**
- Análise por município
- Disparidades regionais
- Planejamento territorial

---

## 🚀 **Vantagens do Modelo**

### ⚡ **Performance:**
- Agregações pré-calculadas
- Consultas otimizadas para BI
- Junções simplificadas

### 📋 **Consistência:**
- Indicadores padronizados
- Fórmulas validadas
- Metadados ricos

### 🔄 **Flexibilidade:**
- Adição de novas dimensões
- Expansão de indicadores
- Suporte a históricos

---

## 📊 **View Analítica:**
**`gold_indicadores_saude`**
- 17 colunas com indicadores calculados
- Percentuais e taxas prontos
- Otimizada para dashboards

---

## 🎯 **Impacto Business Intelligence**

> **Dados prontos para análise estratégica** com modelo dimensional que permite:
> - Monitoramento de indicadores de saúde
> - Tomada de decisão baseada em evidências  
> - Identificação de desigualdades regionais
> - Avaliação de políticas públicas

**Status:** ✅ Produção - Modelo completo para consumo analítico

<img width="1000" height="1000" alt="dbexpert-schema (1)" src="https://github.com/user-attachments/assets/15456bfc-3733-47a0-b06d-c41fc02b1519" />


