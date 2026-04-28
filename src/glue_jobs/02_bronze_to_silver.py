import sys
import csv
import io
import os
import re
import tempfile
from pathlib import Path
from datetime import datetime

from pyspark.sql.functions import col

import boto3
import numpy as np

from brukeropusreader import read_file as read_opus

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import Row
from pyspark.sql import functions as F



# Iniciando Glue e Spark
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.sparkContext.setLogLevel("WARN")



# Configuracoes
AWS_REGION = "us-east-1"
BUCKET = "projeto-edb-015"

BRONZE_PREFIX = "bronze/afsis/"
SILVER_PREFIX = "silver/afsis/"
VALIDATIONS_PREFIX = "validations/silver/afsis/"

# Deixar em branco para capturar tudo que está na base bronze
SOURCE_SUBPREFIX = ""

# Saída final
SILVER_MASTER_PARQUET_PATH = f"s3://{BUCKET}/{SILVER_PREFIX}silver_master_afsis_parquet/"
SILVER_MASTER_CSV_PATH = f"s3://{BUCKET}/{SILVER_PREFIX}silver_master_afsis_csv/"

TARGET_COLS = [
    "pH",
    "%C",
    " Olsen P mg/kg",
    "ECEC cmolc/ kg soil",
    "ExBas"
]

s3 = boto3.client("s3", region_name=AWS_REGION)


# S3
def list_s3_keys(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/"):
                yield key


def download_s3_to_tempfile(bucket, key):
    tmp = tempfile.NamedTemporaryFile(delete=False)
    tmp.close()
    s3.download_file(bucket, key, tmp.name)
    return tmp.name


# Funções de negócio
def extrair_sample_id_do_nome_arquivo(nome_arquivo):
    """
    Trata casos como por exemplo: icr005928.0 -> 005928
    """
    stem = Path(nome_arquivo).stem.strip()   # icr005928
    sample_id = stem.replace("icr", "").strip()
    return sample_id


def processar_arquivo_opus(caminho_arquivo, s3_key=None):
    try:
        opus = read_opus(str(caminho_arquivo))

        spec = opus.get("AB", None)

        if spec is None:
            print(f"Sem espectro AB em {Path(caminho_arquivo).name}")
            return None

        y = np.array(spec, dtype=float)
        x = np.arange(len(y))

        row_dict = {f"wn_{int(w)}": float(v) for w, v in zip(x, y)}

        nome_original = Path(s3_key).name if s3_key else Path(caminho_arquivo).name
        sample_id = extrair_sample_id_do_nome_arquivo(nome_original)

        row_dict["sample_id"] = sample_id
        row_dict["SSN"] = "icr" + str(sample_id)
        row_dict["file_name"] = nome_original
        row_dict["source"] = s3_key if s3_key else str(caminho_arquivo)

        return row_dict

    except Exception as e:
        print(f"Erro em {Path(caminho_arquivo).name}: {e}")
        return None


def encontrar_key_por_nome(lista_keys, nome_arquivo):
    for key in lista_keys:
        if key.endswith(nome_arquivo):
            return key
    return None


def ler_csv_spark_por_key(key):
    path = f"s3://{BUCKET}/{key}"
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def padronizar_df_wet(df, nome_df):
    if "SSN" not in df.columns:
        raise KeyError(f"O CSV {nome_df} não possui coluna 'SSN'.")

    return df.withColumn(
        "SSN",
        F.when(F.trim(F.col("SSN")) == "", F.lit(None)).otherwise(F.col("SSN"))
    )


# Análise dos Espectros (espectroscopia)
def construir_base_espectral():
    print("ETAPA 1 - Construindo base espectral dos arquivos Bruker_Alpha_KBr")

    prefix = f"{BRONZE_PREFIX}{SOURCE_SUBPREFIX}"
    todas_as_keys = list(list_s3_keys(BUCKET, prefix))

    opus_keys = [
        k for k in todas_as_keys
        if "Bruker_Alpha_KBr/" in k and re.search(r"\.0$", k)
    ]

    print(f"Arquivos .0 encontrados: {len(opus_keys)}")

    linhas = []
    erros = 0

    for i, key in enumerate(opus_keys, start=1):
        
        # if i > 5:
        #    break
        
        temp_path = None

        try:
            print(f"[ESPECTRO {i}/{len(opus_keys)}] Processando {key}")
            temp_path = download_s3_to_tempfile(BUCKET, key)

            try:
                row = processar_arquivo_opus(temp_path, s3_key=key)
                if row is not None:
                    linhas.append(row)
                else:
                    erros += 1
            except Exception as e:
                erros += 1
                print(f"Erro no arquivo {key}: {e}")

            if i % 100 == 0:
                print(f"Processados {i} arquivos espectrais de {len(opus_keys)}")

        except Exception as e:
            erros += 1
            print(f"Erro ao processar {key}: {e}")

        finally:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)

    if not linhas:
        raise ValueError("Nenhuma linha espectral foi gerada.")

    df_bronze_espectro = spark.createDataFrame(Row(**x) for x in linhas)

    df_bronze_espectro = (
        df_bronze_espectro
        .withColumn(
            "SSN",
            F.when(F.trim(F.col("SSN")) == "", F.lit(None)).otherwise(F.col("SSN"))
        )
        .dropDuplicates(["SSN"])
    )

    print(f"Base espectral criada com {df_bronze_espectro.count()} linhas. Erros: {erros}")
    
    df_bronze_espectro.select("SSN", "file_name").show(10, truncate=False)
    
    return df_bronze_espectro


# Carregando bases "Wet"
def carregar_wet_csvs():
    print("ETAPA 2 - Carregando CSVs Wet")

    prefix = f"{BRONZE_PREFIX}{SOURCE_SUBPREFIX}"
    todas_as_keys = list(list_s3_keys(BUCKET, prefix))

    key_cropnuts = encontrar_key_por_nome(todas_as_keys, "Wet_Chemistry_CROPNUTS.csv")
    key_icraf = encontrar_key_por_nome(todas_as_keys, "Wet_Chemistry_ICRAF.csv")
    key_rres = encontrar_key_por_nome(todas_as_keys, "Wet_Chemistry_RRES.csv")

    print(f"Wet CROPNUTS: {key_cropnuts}")
    print(f"Wet ICRAF: {key_icraf}")
    print(f"Wet RRES: {key_rres}")

    if not key_cropnuts:
        raise FileNotFoundError("Wet_Chemistry_CROPNUTS.csv não encontrado.")
    if not key_icraf:
        raise FileNotFoundError("Wet_Chemistry_ICRAF.csv não encontrado.")
    if not key_rres:
        raise FileNotFoundError("Wet_Chemistry_RRES.csv não encontrado.")

    df_wet_cropnuts = ler_csv_spark_por_key(key_cropnuts)
    df_wet_icraf = ler_csv_spark_por_key(key_icraf)
    df_wet_rres = ler_csv_spark_por_key(key_rres)

    df_wet_cropnuts = padronizar_df_wet(df_wet_cropnuts, "cropnuts")
    df_wet_icraf = padronizar_df_wet(df_wet_icraf, "icraf")
    df_wet_rres = padronizar_df_wet(df_wet_rres, "rres")

    print("CSVs Wet carregados com sucesso")
    return df_wet_cropnuts, df_wet_icraf, df_wet_rres


# Merge entre as tabelas
def construir_silver_master(
    df_bronze_espectro,
    df_wet_cropnuts,
    df_wet_icraf,
    df_wet_rres
):
    print("ETAPA 3 - Fazendo merge final")

    r = df_wet_rres.alias("r")
    i = df_wet_icraf.alias("i")
    c = df_wet_cropnuts.alias("c")

    # FULL OUTER entre as três bases wet
    df_wet_join = (
        r.join(i, on="SSN", how="full_outer")
         .join(c, on="SSN", how="full_outer")
    )

    # Monta seleção final das colunas-alvo usando coalesce
    select_exprs = [F.col("SSN")]

    for col_name in TARGET_COLS:
        fontes = []

        if col_name in df_wet_rres.columns:
            fontes.append(F.col(f"r.`{col_name}`"))
        if col_name in df_wet_icraf.columns:
            fontes.append(F.col(f"i.`{col_name}`"))
        if col_name in df_wet_cropnuts.columns:
            fontes.append(F.col(f"c.`{col_name}`"))

        if fontes:
            select_exprs.append(F.coalesce(*fontes).alias(col_name))

    df_targets = df_wet_join.select(*select_exprs)

    # Merge com espectro
    df_silver = df_targets.join(df_bronze_espectro, on="SSN", how="left")

    # remove colunas que você já remove localmente
    cols_drop = [
        "sample_id",
        "file_name",
        "sample_name_param",
        "instrument",
        "resolution",
        "scans",
        "source",
        "experiment",
        "product",
        "product_group",
        "fd1",
        "wn_min",
        "wn_max",
        "n_points",
    ]

    cols_drop = [c for c in cols_drop if c in df_silver.columns]
    if cols_drop:
        df_silver = df_silver.drop(*cols_drop)

    df_silver = (
        df_silver
        .withColumn(
            "SSN",
            F.when(F.trim(F.col("SSN")) == "", F.lit(None)).otherwise(F.col("SSN"))
        )
        .dropDuplicates(["SSN"])
    )

    print(f"Base silver final com {df_silver.count()} linhas e {len(df_silver.columns)} colunas")
    
    # Padronização das colunas
    def padronizar_nome(col_name):
        col_name = col_name.strip().lower()

        # regras específicas de negócio
        #mapping = {
        #    "%C": "carbon_pct",
        #}

        col_clean = col_name.replace(" ", "").replace("%", "").lower()

        #if col_clean in mapping:
        #    return mapping[col_clean]

        # regras gerais
        col_name = col_name.replace("%", "pct_")
        col_name = col_name.replace("/", "_")
        col_name = col_name.replace(" ", "_")
        col_name = re.sub(r"[^a-z0-9_]", "", col_name)

        return col_name

    df_silver = df_silver.select([
        col(c).alias(padronizar_nome(c)) for c in df_silver.columns
    ])
    
    
    return df_silver



# Salvando a base Silver
def salvar_silver(df_silver):
    print("ETAPA 4 - Salvando silver")

    try:
        df_silver.coalesce(1).write.mode("overwrite").parquet(SILVER_MASTER_PARQUET_PATH)
        print(f"Silver salva em {SILVER_MASTER_PARQUET_PATH}")

        # Renomeando arquivo parquet
        s3 = boto3.client("s3")

        response = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix=f"{SILVER_PREFIX}silver_master_afsis_parquet/"
        )

        parquet_file = [
            obj["Key"]
            for obj in response["Contents"]
            if obj["Key"].endswith(".parquet")
        ][0]

        novo_nome = f"{SILVER_PREFIX}silver_master_afsis.parquet"

        s3.copy_object(
            Bucket=BUCKET,
            CopySource={"Bucket": BUCKET, "Key": parquet_file},
            Key=novo_nome
        )

        print(f"Arquivo final salvo como: s3://{BUCKET}/{novo_nome}")

    except Exception as e:
        print(f"Falha ao salvar parquet ({e}). Salvando CSV como fallback.")
        (
            df_silver
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(SILVER_MASTER_CSV_PATH)
        )
        print(f"Silver salva em {SILVER_MASTER_CSV_PATH}")


def calcular_metricas_validacao(
    df_bronze_espectro,
    df_wet_cropnuts,
    df_wet_icraf,
    df_wet_rres,
    df_silver
):
    print("ETAPA 5 - Calculando métricas de validação")

    metricas = []

    def add_metrica(nome, valor):
        metricas.append({
            "metric_name": nome,
            "metric_value": str(valor)
        })

    # 1) Volumetria básica
    add_metrica("qtd_linhas_bronze_espectro", df_bronze_espectro.count())
    add_metrica("qtd_linhas_wet_cropnuts", df_wet_cropnuts.count())
    add_metrica("qtd_linhas_wet_icraf", df_wet_icraf.count())
    add_metrica("qtd_linhas_wet_rres", df_wet_rres.count())
    add_metrica("qtd_linhas_silver_final", df_silver.count())
    add_metrica("qtd_colunas_silver_final", len(df_silver.columns))

    # 2) Null / vazio em SSN
    add_metrica(
        "qtd_ssn_nulo_bronze_espectro",
        df_bronze_espectro.filter(F.col("SSN").isNull()).count()
    )
    add_metrica(
        "qtd_ssn_nulo_wet_cropnuts",
        df_wet_cropnuts.filter(F.col("SSN").isNull()).count()
    )
    add_metrica(
        "qtd_ssn_nulo_wet_icraf",
        df_wet_icraf.filter(F.col("SSN").isNull()).count()
    )
    add_metrica(
        "qtd_ssn_nulo_wet_rres",
        df_wet_rres.filter(F.col("SSN").isNull()).count()
    )

    # 3) Duplicidade por SSN
    dup_bronze = (
        df_bronze_espectro.groupBy("SSN")
        .count()
        .filter((F.col("SSN").isNotNull()) & (F.col("count") > 1))
        .count()
    )

    dup_cropnuts = (
        df_wet_cropnuts.groupBy("SSN")
        .count()
        .filter((F.col("SSN").isNotNull()) & (F.col("count") > 1))
        .count()
    )

    dup_icraf = (
        df_wet_icraf.groupBy("SSN")
        .count()
        .filter((F.col("SSN").isNotNull()) & (F.col("count") > 1))
        .count()
    )

    dup_rres = (
        df_wet_rres.groupBy("SSN")
        .count()
        .filter((F.col("SSN").isNotNull()) & (F.col("count") > 1))
        .count()
    )

    dup_silver = (
        df_silver.groupBy("ssn")
        .count()
        .filter((F.col("ssn").isNotNull()) & (F.col("count") > 1))
        .count()
    )

    add_metrica("qtd_ssn_duplicado_bronze_espectro", dup_bronze)
    add_metrica("qtd_ssn_duplicado_wet_cropnuts", dup_cropnuts)
    add_metrica("qtd_ssn_duplicado_wet_icraf", dup_icraf)
    add_metrica("qtd_ssn_duplicado_wet_rres", dup_rres)
    add_metrica("qtd_ssn_duplicado_silver_final", dup_silver)

    # 4) Cobertura do join wet x espectro
    df_wet_unificado_ssn = (
        df_wet_cropnuts.select("SSN")
        .union(df_wet_icraf.select("SSN"))
        .union(df_wet_rres.select("SSN"))
        .filter(F.col("SSN").isNotNull())
        .dropDuplicates(["SSN"])
    )

    qtd_ssn_wet_unificado = df_wet_unificado_ssn.count()
    qtd_ssn_espectro = (
        df_bronze_espectro
        .filter(F.col("SSN").isNotNull())
        .select("SSN")
        .dropDuplicates(["SSN"])
        .count()
    )

    qtd_wet_com_match_espectro = (
        df_wet_unificado_ssn.alias("w")
        .join(
            df_bronze_espectro.select("SSN").dropDuplicates(["SSN"]).alias("e"),
            on="SSN",
            how="inner"
        )
        .count()
    )

    qtd_wet_sem_espectro = qtd_ssn_wet_unificado - qtd_wet_com_match_espectro

    qtd_espectro_sem_wet = (
        df_bronze_espectro.select("SSN").dropDuplicates(["SSN"]).alias("e")
        .join(df_wet_unificado_ssn.alias("w"), on="SSN", how="left_anti")
        .count()
    )

    cobertura = (
        round((qtd_wet_com_match_espectro / qtd_ssn_wet_unificado) * 100, 2)
        if qtd_ssn_wet_unificado > 0 else 0
    )

    add_metrica("qtd_ssn_unicos_wet_unificado", qtd_ssn_wet_unificado)
    add_metrica("qtd_ssn_unicos_espectro", qtd_ssn_espectro)
    add_metrica("qtd_wet_com_match_espectro", qtd_wet_com_match_espectro)
    add_metrica("qtd_wet_sem_espectro", qtd_wet_sem_espectro)
    add_metrica("qtd_espectro_sem_wet", qtd_espectro_sem_wet)
    add_metrica("pct_cobertura_wet_com_espectro", cobertura)

    # 5) Campos alvo na silver
    target_cols_silver = [
        "ph",
        "pct_c",
        "olsen_p_mg_kg",
        "ecec_cmolc_kg_soil",
        "exbas"
    ]

    for c in target_cols_silver:
        if c in df_silver.columns:
            qtd_preenchido = df_silver.filter(F.col(c).isNotNull()).count()
            qtd_nulo = df_silver.filter(F.col(c).isNull()).count()
            add_metrica(f"qtd_preenchido_{c}", qtd_preenchido)
            add_metrica(f"qtd_nulo_{c}", qtd_nulo)


    # 6) Regras simples de domínio
    if "ph" in df_silver.columns:
        add_metrica(
            "qtd_ph_fora_faixa_0_14",
            df_silver.filter(
                F.col("ph").isNotNull() & ((F.col("ph") < 0) | (F.col("ph") > 14))
            ).count()
        )

    if "pct_c" in df_silver.columns:
        add_metrica(
            "qtd_pct_c_negativo",
            df_silver.filter(
                F.col("pct_c").isNotNull() & (F.col("pct_c") < 0)
            ).count()
        )

    if "olsen_p_mg_kg" in df_silver.columns:
        add_metrica(
            "qtd_olsen_p_mg_kg_negativo",
            df_silver.filter(
                F.col("olsen_p_mg_kg").isNotNull() & (F.col("olsen_p_mg_kg") < 0)
            ).count()
        )

    if "ecec_cmolc_kg_soil" in df_silver.columns:
        add_metrica(
            "qtd_ecec_cmolc_kg_soil_negativo",
            df_silver.filter(
                F.col("ecec_cmolc_kg_soil").isNotNull() & (F.col("ecec_cmolc_kg_soil") < 0)
            ).count()
        )

    if "exbas" in df_silver.columns:
        add_metrica(
            "qtd_exbas_negativo",
            df_silver.filter(
                F.col("exbas").isNotNull() & (F.col("exbas") < 0)
            ).count()
        )

    # 7) Estrutura espectral
    wn_cols = [c for c in df_silver.columns if c.startswith("wn_")]
    add_metrica("qtd_colunas_espectrais_wn", len(wn_cols))

    for c in wn_cols[:10]:
        # opcional: só registra as 10 primeiras para não poluir
        add_metrica(
            f"qtd_nulo_{c}",
            df_silver.filter(F.col(c).isNull()).count()
        )

    return metricas

def salvar_validacoes_csv(metricas):
    print("ETAPA 6 - Salvando relatório de validação")

    timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M")
    file_name = f"validations_silver_{timestamp}.csv"
    s3_key = f"{VALIDATIONS_PREFIX}{file_name}"

    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=["metric_name", "metric_value"])
    writer.writeheader()
    writer.writerows(metricas)

    s3.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv"
    )

    print(f"Validação salva em: s3://{BUCKET}/{s3_key}")
    

# Função MAIN
def main():
    inicio = datetime.utcnow()
    print("JOB SILVER INICIOU")

    df_bronze_espectro = construir_base_espectral()

    (
        df_wet_cropnuts,
        df_wet_icraf,
        df_wet_rres
    ) = carregar_wet_csvs()

    df_silver = construir_silver_master(
        df_bronze_espectro=df_bronze_espectro,
        df_wet_cropnuts=df_wet_cropnuts,
        df_wet_icraf=df_wet_icraf,
        df_wet_rres=df_wet_rres
    )

    salvar_silver(df_silver)

    metricas = calcular_metricas_validacao(
        df_bronze_espectro=df_bronze_espectro,
        df_wet_cropnuts=df_wet_cropnuts,
        df_wet_icraf=df_wet_icraf,
        df_wet_rres=df_wet_rres,
        df_silver=df_silver
    )

    salvar_validacoes_csv(metricas)

    fim = datetime.utcnow()
    print(f"JOB SILVER FINALIZOU em {(fim - inicio).total_seconds():.2f} segundos")


if __name__ == "__main__":
    main()
    job.commit()