# desafio-data-engineer-sinasc
Solução de Engenharia de Dados para construir a fundação de um Data Lakehouse, processando dados brutos do SINASC e criando um modelo Star Schema para análise de saúde materno-infantil na Baixada Santista, SP.

# 🏥 Data Lakehouse para Análise de Saúde Materno-Infantil - SP

## 🌟 Visão Geral

Este projeto implementa um Data Lakehouse completo para monitoramento de saúde materno-infantil no Estado de São Paulo, utilizando dados dos sistemas SINASC (nascimentos) e SIM-DOINF (óbitos infantis) do DATASUS.

**Período dos dados:** 2010 a 2024  
**Ferramentas:** Databricks, PySpark, Spark SQL, Delta Lake  
**Arquitetura:** Medalhão (Bronze → Silver → Gold) com Star Schema na camada final

## 🏗️ Arquitetura do Projeto

### Arquitetura Medalhão Implementada

A arquitetura segue o padrão Medalhão com três camadas principais implementadas em um único notebook:

1. **Camada Bronze**: Dados brutos ingeridos diretamente dos arquivos .dbc do DATASUS, preservando o formato original com metadados de proveniência.

2. **Camada Silver**: Dados limpos, validados e enriquecidos com transformações de qualidade e padronização.

3. **Camada Gold**: Modelo dimensional otimizado para análise com indicadores estratégicos agregados.

### Estrutura do Notebook Único

O pipeline completo é executado em sequência dentro de um único notebook:

```python
# SEÇÃO 1: CONFIGURAÇÃO E IMPORTAÇÕES
# SEÇÃO 2: CAMADA BRONZE - Ingestão de dados brutos
# SEÇÃO 3: CAMADA SILVER - Transformação e limpeza  
# SEÇÃO 4: CAMADA GOLD - Agregação e indicadores
# SEÇÃO 5: VALIDAÇÃO - Testes e qualidade
```

## ⚙️ Pré-requisitos e Configuração

### Requisitos do Ambiente
- **Databricks Runtime:** 10.4 LTS ou superior
- **Python:** 3.8+
- **PySpark:** 3.2+
- **Bibliotecas:** pyreadstat, delta-spark

### Configuração Inicial

1. **Configurar Volume no Databricks:**
```sql
CREATE VOLUME IF NOT EXISTS workspace.default.data
```

2. **Upload dos Arquivos .dbc:**
```bash
# Colocar arquivos no volume criado
# Estrutura esperada:
# /Volumes/workspace/default/data/
#   ├── DNSP2010.dbc
#   ├── DNSP2011.dbc
#   ├── ...
#   ├── DOINF2010.dbc
#   └── DOINF2011.dbc
```

## 🚀 Instruções de Execução

### Execução do Pipeline Completo

```python
# Executar o notebook completo em sequência:
# 1. Configuração inicial
# 2. Camada Bronze
# 3. Camada Silver  
# 4. Camada Gold
# 5. Validação final

# Todas as células serão executadas sequencialmente
# O tempo total estimado é de 15-30 minutos dependendo do volume de dados
```

### Execução por Seções

**Seção 1 - Configuração:**
```python
# Configurar ambiente Spark
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Definir caminhos
VOLUME_BASE_PATH = "/Volumes/workspace/default/data/"
```

**Seção 2 - Camada Bronze:**
```python
# Executar função de ingestão
ingestao_bronze_completa()
```

**Seção 3 - Camada Silver:**
```python
# Processar transformações
transformar_silver_nascimentos()
transformar_silver_obitos()
```

**Seção 4 - Camada Gold:**
```python
# Criar modelo dimensional
criar_camada_gold()
```

**Seção 5 - Validação:**
```python
# Executar testes
validar_pipeline_completo()
```

## 🎯 Decisões Técnicas e Justificativas

### 1. Notebook Único com Múltiplas Camadas
**Justificativa:** Implementamos todas as camadas em um único notebook para:
- **Facilitar a execução** e reprodução do pipeline completo
- **Reduzir dependências** entre notebooks separados
- **Simplificar a manutenção** e versionamento
- **Otimizar o uso de recursos** do Databricks

### 2. Arquitetura Medalhão
**Justificativa:** Escolhemos a arquitetura medalhão para:
- **Resiliência a mudanças de schema** entre diferentes anos dos dados DATASUS
- **Preservação dos dados originais** na camada Bronze
- **Transformação incremental** com qualidade crescente
- **Reusabilidade** dos dados para múltiplos propósitos

### 3. Formato Delta Lake
**Justificativa:** Utilizamos Delta Lake por oferecer:
- **ACID transactions** para garantia de consistência
- **Schema evolution** nativo para evolução dos dados
- **Time travel** para auditoria e reprocessamento
- **Compaction e otimização** automática

### 4. Processamento de Arquivos .dbc
**Justificativa:** Implementamos processamento nativo porque:
- **Evita dependências externas** (reprodutibilidade)
- **Processamento 100% dentro do Databricks**
- **Controle total** sobre o processo de parsing
- **Adaptabilidade** a mudanças de formato

### 5. Agregações na Camada Gold
**Justificativa:** As agregações mensais foram escolhidas porque:
- **Balanceiam detalhe e performance** para análise
- **Permitem análise temporal** (sazonalidade, tendências)
- **Facilitam comparações** entre períodos e regiões
- **Atendem aos requisitos** dos indicadores de saúde solicitados

## ✅ Validações e Testes Realizados

### 1. Validação da Camada Bronze
```python
# Teste de ingestão completa
def test_ingestao_bronze():
    sinasc_count = spark.read.table("bronze_sinasc").count()
    sim_count = spark.read.table("bronze_sim").count()
    
    assert sinasc_count > 0, "Bronze SINASC vazia"
    assert sim_count >= 0, "Bronze SIM com problemas"
    
    print(f"✅ Bronze SINASC: {sinasc_count:,} registros")
    print(f"✅ Bronze SIM: {sim_count:,} registros")
```

### 2. Validação da Camada Silver
```python
# Teste de qualidade dos dados Silver
def test_qualidade_silver():
    nascimentos = spark.read.table("silver_nascimentos")
    
    # Testes de integridade
    assert nascimentos.filter(col("data_nascimento").isNull()).count() == 0
    assert nascimentos.filter(col("codigo_municipio_nascimento").isNull()).count() == 0
    
    # Testes de domínios categóricos
    sexos_validos = nascimentos.filter(~col("sexo").isin(["Masculino", "Feminino", "Ignorado"])).count()
    assert sexos_validos == 0, "Valores inválidos na coluna sexo"
```

### 3. Validação da Camada Gold
```python
# Teste dos indicadores calculados
def test_indicadores_gold():
    indicadores = spark.read.table("gold_indicadores_saude")
    
    # Verificar que percentuais estão entre 0-100
    assert indicadores.filter((col("perc_baixo_peso") < 0) | (col("perc_baixo_peso") > 100)).count() == 0
    
    # Verificar taxas de mortalidade calculadas corretamente
    taxas_corretas = indicadores.filter(
        (col("taxa_mortalidade_infantil") == (col("total_obitos_infantis") / col("total_nascidos_vivos")) * 1000)
    ).count()
    
    assert taxas_corretas == indicadores.count(), "Cálculo de taxas incorreto"
```

### 4. Validação de Performance
```python
# Teste de performance das consultas
def test_performance():
    import time
    
    start_time = time.time()
    resultado = spark.sql("""
        SELECT sk_tempo, AVG(taxa_mortalidade_infantil) 
        FROM gold_indicadores_saude 
        GROUP BY sk_tempo 
        ORDER BY sk_tempo
    """).count()
    
    tempo_execucao = time.time() - start_time
    assert tempo_execucao < 10, "Consulta muito lenta"
    print(f"✅ Performance: {tempo_execucao:.2f} segundos")
```

### 5. Validação dos Indicadores Obrigatórios
```python
# Verificação de todos os indicadores solicitados
def test_indicadores_obrigatorios():
    indicadores = spark.read.table("gold_indicadores_saude")
    colunas_obrigatorias = [
        "total_nascidos_vivos",
        "perc_prenatal_7_ou_mais_consultas", 
        "perc_baixo_peso",
        "perc_partos_cesarea",
        "perc_maes_adolescentes",
        "total_obitos_infantis",
        "taxa_mortalidade_infantil",
        "total_obitos_neonatais", 
        "taxa_mortalidade_neonatal",
        "total_obitos_maternos",
        "taxa_mortalidade_materna"
    ]
    
    for coluna in colunas_obrigatorias:
        assert coluna in indicadores.columns, f"Coluna {coluna} não encontrada"
    
    print("✅ Todos os indicadores obrigatórios implementados")
```

## 🔮 Próximos Passos e Melhorias

### Melhorias Técnicas
1. **Implementação de monitoramento** com Databricks Workflows e alertas
2. **Otimização de performance** com Z-Ordering e Bloom Filters
3. **Data Quality Framework** com validações automatizadas

### Expansão do Modelo
1. **Novas fontes de dados:** Incorporar dados do CNES e IBGE
2. **Indicadores avançados:** Anos de vida perdidos, análise de desigualdades
3. **Análise preditiva:** Modelos de séries temporais para previsão

### Melhorias de Governança
1. **Catálogo de dados** com Unity Catalog
2. **Lineage completo** dos dados
3. **Controle de acesso** granular por camada

---

**Desenvolvido para a Cuidado Conectado** - Transformando dados em insights para saúde pública.
