# 🥇 Camada Gold - Modelo Dimensional para Saúde Materno-Infantil

## 🎯 **Objetivo Principal**
Modelagem dimensional em Star Schema otimizada para análise estratégica de **indicadores de saúde materno-infantil** com agregações pré-calculadas para Business Intelligence.

---

## 🏗️ **Arquitetura do Modelo Dimensional**

### ⭐ **Tabela Fato Central**
**`gold_fato_saude_mensal_cnes`**
- **Granularidade:** Mensal por estabelecimento (CNES) e município
- **Chaves Dimensionais:** `sk_tempo`, `sk_cnes`, `sk_municipio`
- **Métricas:** 15 indicadores estratégicos de saúde
- **Registros:** ~17K combinações únicas (CNES + Município + Mês)

### 📐 **Dimensões Conformadas**
| Dimensão | Descrição | Elementos |
|:---|:---|:---|
| **⏰ gold_dim_tempo** | Períodos mensais (12 meses) | ano, mes, ano_mes_formatado |
| **🏥 gold_dim_cnes** | Estabelecimentos de saúde (~3.3K) | código CNES normalizado |
| **🗺️ gold_dim_municipio** | Municípios brasileiros (~2K) | código, nome, UF, região, porte |

---

## 📈 **Indicadores Estratégicos Implementados**

### 👶 **SAÚDE MATERNO-INFANTIL (NASCIMENTOS)**
| Indicador | Descrição | Fórmula |
|:---|:---|:---|
| **`total_nascidos_vivos`** | Volume absoluto de nascimentos | COUNT(*) |
| **`nascidos_7_consultas`** | Pré-natal adequado (7+ consultas) | COUNT(consultas_pre_natal ≥ 7) |
| **`nascidos_baixo_peso`** | Recém-nascidos <2500g | COUNT(peso_gramas < 2500) |
| **`nascidos_baixissimo_peso`** | Recém-nascidos <1500g | COUNT(peso_gramas < 1500) |
| **`nascidos_partos_cesarea`** | Partos cesáreos | COUNT(tipo_parto = 'Cesáreo') |
| **`nascidos_maes_adolescentes`** | Mães adolescentes (<20 anos) | COUNT(idade_mae < 20) |
| **`nascidos_pre_termo`** | Nascimentos pré-termo (<37 semanas) | COUNT(semanas_gestacao < 37) |
| **`nascidos_prenatal_adequado`** | Pré-natal adequado | COUNT(consultas_pre_natal ≥ 7) |

### ⚠️ **INDICADORES DE MORTALIDADE (ÓBITOS)**
| Indicador | Descrição | Fórmula |
|:---|:---|:---|
| **`total_obitos`** | Total de óbitos registrados | COUNT(*) |
| **`total_obitos_infantis`** | Óbitos infantis (<1 ano) | COUNT(idade < 1) |
| **`total_obitos_neonatais`** | Óbitos neonatais (<28 dias) | COUNT(idade < 28) |
| **`total_obitos_maternos`** | Óbitos maternos | COUNT(causa_basica LIKE 'O%') |

---

## 📊 **View de Indicadores Calculados**

**`gold_indicadores_saude`** - 28 colunas com métricas prontas para análise:

### 📈 **Taxas e Percentuais (Cálculos Dinâmicos)**
```sql
-- Qualidade do Pré-natal
perc_prenatal_7_ou_mais_consultas = (nascidos_7_consultas / total_nascidos_vivos) * 100
perc_prenatal_adequado = (nascidos_prenatal_adequado / total_nascidos_vivos) * 100

-- Resultados Perinatais
perc_baixo_peso_total = ((nascidos_baixo_peso + nascidos_baixissimo_peso) / total_nascidos_vivos) * 100
perc_baixo_peso = (nascidos_baixo_peso / total_nascidos_vivos) * 100
perc_baixissimo_peso = (nascidos_baixissimo_peso / total_nascidos_vivos) * 100
perc_pre_termo = (nascidos_pre_termo / total_nascidos_vivos) * 100

-- Procedimentos Obstétricos
perc_partos_cesarea = (nascidos_partos_cesarea / total_nascidos_vivos) * 100

-- Perfil Sociodemográfico
perc_maes_adolescentes = (nascidos_maes_adolescentes / total_nascidos_vivos) * 100

-- Mortalidade (Taxas por mil/nascidos)
taxa_mortalidade_infantil = (total_obitos_infantis / total_nascidos_vivos) * 1000
taxa_mortalidade_neonatal = (total_obitos_neonatais / total_nascidos_vivos) * 1000
taxa_mortalidade_materna = (total_obitos_maternos / total_nascidos_vivos) * 100000
```

---

## 🔍 **Dimensões de Análise Habilitadas**

### ⏰ **Temporal**
- Agregação mensal (12 períodos)
- Análise de tendências e sazonalidade
- Comparativos interanuais

### 🏥 **Por Estabelecimento de Saúde**
- Desempenho por CNES (~3.3K unidades)
- Benchmarking entre unidades
- Identificação de melhores práticas

### 🗺️ **Geográfica**
- Análise por município (~2K municípios)
- Disparidades regionais e interestaduais
- Planejamento territorial da saúde

---

## ⚡ **Otimizações Implementadas**

### 🔄 **Processamento Eficiente**
- **Agregação pré-calculada** para performance de consulta
- **Full outer join** entre nascimentos e óbitos para cobertura completa
- **Preenchimento de nulos** com zero para cálculos seguros
- **Compactação Delta** com `OPTIMIZE` e estatísticas

### 📊 **Consumo Analítico Otimizado**
- **View materializada** com todos os indicadores calculados
- **Chaves dimensionais** padronizadas para joins eficientes
- **Filtros otimizados** por período e localidade

---

## 🚀 **Pronto para Análise Estratégica**

**Status:** ✅ Produção - Modelo dimensional completo para:

### 📈 **Dashboards e Relatórios**
- Monitoramento de indicadores do SUS em tempo real
- Acompanhamento de metas de qualidade da atenção materno-infantil
- Alinhamento com ODS (Objetivos de Desenvolvimento Sustentável)

### 🔍 **Análises Estratégicas**
- Tendências temporais (séries históricas)
- Comparativos regionais e municipais
- Análise de equidade e disparidades na saúde

### 🎯 **Consultas Exemplo**
```sql
-- Top 10 municípios com maior taxa de cesárea
SELECT * FROM gold_indicadores_saude 
ORDER BY perc_partos_cesarea DESC LIMIT 10

-- Evolução mensal da mortalidade infantil
SELECT sk_tempo, SUM(total_obitos_infantis) as obitos, 
       SUM(total_nascidos_vivos) as nascidos
FROM gold_fato_saude_mensal_cnes 
GROUP BY sk_tempo ORDER BY sk_tempo

-- Qualidade do pré-natal por região
SELECT regiao, AVG(perc_prenatal_adequado) as pre_natal_medio
FROM gold_indicadores_saude i
JOIN gold_dim_municipio m ON i.sk_municipio = m.id_municipio  
GROUP BY regiao
```

---

## 📋 **Metadados Técnicos**

**Database:** `default`  
**Formato:** Delta Lake
**Granularidade:** Mensal por estabelecimento de saúde
**Performance:** Consultas subsegundo para agregados
**Disponibilidade:** Dados prontos para consumo analítico

> **Impacto Business Intelligence:** Dados estruturados para tomada de decisão baseada em evidências, identificação de desigualdades regionais e avaliação de políticas públicas de saúde.



-------

<img width="1000" height="1000" alt="dbexpert-schema (2)" src="https://github.com/user-attachments/assets/43b28abb-525d-4f4c-833c-9bfe536e5842" />

