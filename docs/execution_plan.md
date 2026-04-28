# Plano de Execução

## 1. Preparação

1. Criar ou confirmar bucket S3 privado.
2. Confirmar permissões de leitura no bucket público `afsis`.
3. Criar os jobs no AWS Glue.
4. Instalar dependências necessárias, incluindo leitor OPUS.

## 2. Execução dos jobs

### Job 1 — Bronze

Executar `01_afsis_to_bronze.py`.

Resultado esperado: arquivos copiados para `bronze/afsis/` e validação em `validations/bronze/afsis/`.

### Job 2 — Silver

Executar `02_bronze_to_silver.py`.

Resultado esperado: base única `silver_master_afsis.parquet` e validação em `validations/silver/afsis/`.

### Job 3 — Gold

Executar `03_silver_to_gold.py`.

Resultado esperado: cinco bases model-ready, uma por target, e validação em `validations/gold/afsis/`.

## 3. Modelagem

Executar `notebooks/main_v6.ipynb`.

O notebook lê as bases Gold, treina modelos e avalia a performance preditiva para cada variável-alvo.
