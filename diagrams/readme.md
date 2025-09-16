# 🥇 Camada Gold - Camada de Agregação e Indicadores Estratégicos

## 🎯 **Objetivo Principal**
Agregação dos dados refinados da camada Silver para cálculo de **indicadores estratégicos de saúde pública** com foco em análise temporal, comparativa e territorial.

---

## 📊 **O que é a Camada Gold?**

A camada Gold representa o **nível mais alto de maturidade dos dados**, onde informações são:
- ✅ **Agregadas** por dimensões estratégicas (tempo, local, tipo)
- ✅ **Transformadas** em indicadores e métricas de negócio
- ✅ **Otimizadas** para consultas analíticas e visualização
- ✅ **Consolidadas** para tomada de decisão

---

## 🏗️ **Estrutura da Camada Gold**

### 📦 **Tabela Fato Principal:**
**`gold_fato_saude_mensal_cnes`**
- Agregação mensal por estabelecimento de saúde (CNES) e município
- 12 colunas de métricas estratégicas
- Chaves dimensionais para integração

### 📊 **View Analítica:**
**`gold_indicadores_saude`**
- 17 colunas com indicadores calculados
- Percentuais e taxas prontos para análise
- Visualização direta para dashboards

---

## 📈 **Principais Indicadores Calculados**

### 👶 **Indicadores de Nascimento:**
| Indicador | Fórmula | Significado |
|:---|:---|:---|
| **Nascidos Vivos** | Contagem absoluta | Volume de atendimentos |
| **Pré-natal Adequado** | (7+ consultas/Total) × 100 | Qualidade do cuidado pré-natal |
| **Baixo Peso** | (<2500g/Total) × 100 | Indicador de condições de vida |
| **Partos Cesáreos** | (Cesáreas/Total) × 100 | Padrões de prática obstétrica |
| **Mães Adolescentes** | (<20 anos/Total) × 100 | Gravidez na adolescência |

### ⚰️ **Indicadores de Mortalidade:**
| Indicador | Fórmula | Significado |
|:---|:---|:---|
| **Mortalidade Infantil** | (Óbitos <1 ano/Nascidos) × 1000 | Principal indicador de saúde |
| **Mortalidade Neonatal** | (Óbitos <28 dias/Nascidos) × 1000 | Qualidade do cuidado perinatal |
| **Mortalidade Materna** | (Óbitos maternos/Nascidos) × 100.000 | Qualidade da assistência obstétrica |

---

## 🔍 **Dimensões de Análise**

### ⏰ **Temporal:**
- Agregação mensal (`ano_mes`)
- Análise de sazonalidade
- Tendências temporais

### 🏥 **Geográfica:**
- Por estabelecimento de saúde (`codigo_cnes`)
- Por município (`codigo_municipio`)
- Análise comparativa entre regiões

### 👥 **Demográfica:**
- Faixa etária materna
- Tipo de parto
- Adequação do pré-natal

---

## 🚀 **Vantagens da Abordagem**

### ⚡ **Performance:**
- Agregações pré-calculadas
- Consultas otimizadas
- Tempo de resposta rápido para dashboards

### 📋 **Consistência:**
- Cálculos padronizados
- Métricas confiáveis
- Fórmulas validadas

### 🔄 **Manutenibilidade:**
- Fácil atualização
- Escalável para novos indicadores
- Documentação clara

---

## 🎨 **Casos de Uso Habilitados**

### 1. **Dashboard de Monitoramento**
- Acompanhamento mensal de indicadores
- Alertas para valores críticos
- Comparativo entre regiões

### 2. **Análise de Desempenho**
- Ranking de estabelecimentos
- Identificação de melhores práticas
- Alocação de recursos

### 3. **Pesquisa e Estudos**
- Correlação entre variáveis
- Estudos temporais
- Publicações científicas

### 4. **Planejamento em Saúde**
- Projeções baseadas em tendências
- Identificação de áreas prioritárias
- Avaliação de políticas públicas

---

## 📊 **Exemplo de Consulta Gold**

```sql
-- Análise de mortalidade infantil por região
SELECT 
    regiao,
    ano,
    SUM(total_nascidos_vivos) as nascimentos,
    SUM(total_obitos_infantis) as obitos_infantis,
    ROUND(AVG(taxa_mortalidade_infantil), 2) as taxa_media
FROM gold_indicadores_saude
JOIN dim_municipios ON sk_municipio = codigo_municipio
GROUP BY regiao, ano
ORDER BY regiao, ano;
```

---

## 🔮 **Próximas Evoluções**

1. **📈 Indicadores Adicionais:**
   - Razão de mortalidade proporcional
   - Indicadores de desigualdade
   - Análise de sobrevivência

2. **🤖 Análise Preditiva:**
   - Modelos de risco
   - Alertas preditivos
   - Simulações de políticas

3. **🌐 Integração:**
   - Dados socioeconômicos
   - Informações de cobertura
   - Integração com outros sistemas

---

## 🎯 **Impacto da Camada Gold**

> **Transformação final dos dados em conhecimento acionável** para gestores, profissionais de saúde e pesquisadores, permitindo decisões baseadas em evidências para melhoria da saúde materno-infantil.

---

**💡 Valor Principal:** Dados prontos para análise estratégica, com indicadores padronizados e confiáveis para tomada de decisão em saúde pública.

<img width="600" height="700" alt="dbexpert-schema" src="https://github.com/user-attachments/assets/9cfc001e-a9fc-4c51-b68f-19b81327b278" />
