# Predição de Wet Chemistry com Dry Chemistry — AfSIS

Este projeto constrói um pipeline de dados e modelagem para prever propriedades de **Wet Chemistry** de solo, que tendem a ser mais caras de medir em laboratório, usando dados de **Dry Chemistry / espectroscopia**, de menor custo, com base no banco público **AfSIS**.

## Objetivo

Criar uma esteira analítica em camadas (**Bronze → Silver → Gold**) e treinar modelos de Machine Learning para estimar variáveis químicas de solo a partir de espectros.

Targets modeladas:

- `ph`
- `pct_c` — carbono percentual
- `olsen_p_mg_kg` — fósforo Olsen
- `ecec_cmolc__kg_soil` — ECEC
- `exbas` — exchangeable bases

## Visão geral da arquitetura

```text
S3 público AfSIS
    ↓
Bronze — cópia bruta dos dados Dry/Wet para S3 privado
    ↓
Silver — leitura dos arquivos OPUS, consolidação espectral e merge com Wet Chemistry
    ↓
Gold — criação de bases model-ready, uma por target
    ↓
Modelagem — treino e comparação de modelos preditivos
```

## Estrutura do repositório

```text
.
├── src/
│   └── glue_jobs/
│       ├── 01_afsis_to_bronze.py
│       ├── 02_bronze_to_silver.py
│       └── 03_silver_to_gold.py
├── notebooks/
│   └── main_v6.ipynb
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   └── execution_plan.md
├── config/
│   └── s3_paths.example.env
├── outputs/
│   └── .gitkeep
├── requirements.txt
├── .gitignore
└── README.md
```

## Pipeline

### 1. Bronze — ingestão bruta

Arquivo: `src/glue_jobs/01_afsis_to_bronze.py`

Responsável por copiar os dados públicos do bucket `afsis` para o bucket privado do projeto, mantendo a estrutura original dos diretórios.

Fontes copiadas:

- `2009-2013/Dry_Chemistry/ICRAF/Bruker_Alpha_KBr/`
- `2009-2013/Wet_Chemistry/CROPNUTS/`
- `2009-2013/Wet_Chemistry/ICRAF/`
- `2009-2013/Wet_Chemistry/RRES/`

Também gera arquivo de validação com status de cópia, tamanho de origem/destino e possíveis falhas.

### 2. Silver — consolidação analítica

Arquivo: `src/glue_jobs/02_bronze_to_silver.py`

Responsável por:

- ler os arquivos espectrais `.0` do Bruker Alpha KBr;
- extrair os vetores espectrais;
- carregar as tabelas Wet Chemistry;
- padronizar `SSN`;
- consolidar as targets;
- fazer merge entre espectro e targets;
- gerar uma base Silver única em Parquet.

Saída principal:

```text
s3://projeto-edb-015/silver/afsis/silver_master_afsis.parquet
```

### 3. Gold — bases model-ready

Arquivo: `src/glue_jobs/03_silver_to_gold.py`

Responsável por criar uma base final para cada variável-alvo. Cada base contém:

- identificador `ssn`;
- target específica;
- colunas espectrais `wn_*`.

Saídas esperadas:

```text
s3://projeto-edb-015/gold/afsis/ph/gold_ph_model_ready.parquet
s3://projeto-edb-015/gold/afsis/carbono/gold_carbono_model_ready.parquet
s3://projeto-edb-015/gold/afsis/p/gold_p_model_ready.parquet
s3://projeto-edb-015/gold/afsis/ecec/gold_ecec_model_ready.parquet
s3://projeto-edb-015/gold/afsis/exbas/gold_exbas_model_ready.parquet
```

### 4. Modelagem

Arquivo: `notebooks/main_v6.ipynb`

Notebook usado para leitura das bases Gold e treinamento dos modelos de regressão para cada target.

## Como executar

### Pré-requisitos

- Conta AWS com acesso ao S3 e AWS Glue
- Bucket privado configurado
- Permissão para leitura do bucket público `afsis`
- Python 3.10+
- Dependências listadas em `requirements.txt`

### Ordem recomendada

1. Executar `01_afsis_to_bronze.py` no AWS Glue.
2. Executar `02_bronze_to_silver.py` no AWS Glue.
3. Executar `03_silver_to_gold.py` no AWS Glue.
4. Abrir e executar `notebooks/main_v6.ipynb` para modelagem.

## Configuração

Os caminhos S3 estão fixados nos scripts para o ambiente original do projeto. Para adaptar a outro ambiente, altere:

```python
BUCKET = "projeto-edb-015"
DEST_BUCKET = "projeto-edb-015"
SILVER_PATH = "s3://projeto-edb-015/silver/afsis/"
GOLD_BASE_PATH = "s3://projeto-edb-015/gold/afsis/"
```

Um exemplo de variáveis está em `config/s3_paths.example.env`.

## Validações geradas

O pipeline gera arquivos de auditoria em:

```text
s3://projeto-edb-015/validations/bronze/afsis/
s3://projeto-edb-015/validations/silver/afsis/
s3://projeto-edb-015/validations/gold/afsis/
```

Essas validações incluem volumetria, duplicidades, nulos, cobertura de joins, status de cópia e regras simples de domínio.

## Observações

- Os dados brutos não são versionados no GitHub.
- Arquivos Parquet, CSV e outputs de modelo devem permanecer fora do repositório.
- O projeto foi estruturado para fins acadêmicos e demonstrativos de Engenharia de Dados + Machine Learning.

## Licença

Defina a licença conforme a necessidade do projeto e as condições de uso do banco AfSIS.
