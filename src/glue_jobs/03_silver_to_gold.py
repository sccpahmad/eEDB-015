import sys
import csv
import io
import boto3
from datetime import datetime
from urllib.parse import urlparse

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

# Parâmetros
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Configurações
SILVER_PATH = "s3://projeto-edb-015/silver/afsis/"
GOLD_BASE_PATH = "s3://projeto-edb-015/gold/afsis/"
VALIDATIONS_PREFIX = "validations/gold/afsis/"

targets = {
    "ph": "ph",
    "carbono": "pct_c",
    "p": "olsen_p_mg_kg",
    "ecec": "ecec_cmolc__kg_soil",
    "exbas": "exbas"
}

id_column = "ssn"

s3_client = boto3.client("s3")


# Funções auxiliares
def parse_s3_uri(s3_uri: str):
    parsed = urlparse(s3_uri)
    return parsed.netloc, parsed.path.lstrip("/")

def delete_prefix(bucket: str, prefix: str):
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    objects = []
    for page in pages:
        for obj in page.get("Contents", []):
            objects.append({"Key": obj["Key"]})

            if len(objects) == 1000:
                s3_client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
                objects = []

    if objects:
        s3_client.delete_objects(Bucket=bucket, Delete={"Objects": objects})

def move_single_parquet_from_temp(temp_s3_path: str, final_s3_path: str):
    temp_bucket, temp_prefix = parse_s3_uri(temp_s3_path)
    final_bucket, final_key = parse_s3_uri(final_s3_path)

    response = s3_client.list_objects_v2(Bucket=temp_bucket, Prefix=temp_prefix)
    contents = response.get("Contents", [])

    parquet_files = [
        obj["Key"]
        for obj in contents
        if obj["Key"].endswith(".parquet") and "part-" in obj["Key"]
    ]

    if len(parquet_files) != 1:
        raise Exception(f"Esperava 1 parquet, encontrou {len(parquet_files)}")

    source_key = parquet_files[0]

    s3_client.copy_object(
        Bucket=final_bucket,
        CopySource={"Bucket": temp_bucket, "Key": source_key},
        Key=final_key
    )

    delete_prefix(temp_bucket, temp_prefix)

def get_object_size(bucket, key):
    return s3_client.head_object(Bucket=bucket, Key=key)["ContentLength"]


# Leitura Silver
df_silver = spark.read.parquet(SILVER_PATH)

print("Schema da silver:")
df_silver.printSchema()

total_silver = df_silver.count()
print(f"Total linhas silver: {total_silver}")

spectral_cols = [c for c in df_silver.columns if c.startswith("wn_")]
print(f"Colunas espectrais: {len(spectral_cols)}")

if id_column not in df_silver.columns:
    raise Exception("Coluna ssn não encontrada.")


# Auditoria

audit_rows = []
execution_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


# Função principal
def build_and_save_gold(df, gold_name, target_col):

    status = "success"
    error_message = ""

    try:
        if target_col not in df.columns:
            raise Exception(f"Target {target_col} não existe")

        selected_cols = [id_column, target_col] + spectral_cols
        selected_cols = [c for c in selected_cols if c in df.columns]

        df_selected = df.select(*selected_cols)

        input_rows = df_selected.count()

        null_target_rows = df_selected.filter(F.col(target_col).isNull()).count()

        df_gold = df_selected.dropna()

        output_rows = df_gold.count()
        dropped_rows = input_rows - output_rows

        pct_dropped = round((dropped_rows / input_rows) * 100, 2) if input_rows > 0 else 0

        duplicate_ssn = (
            df_gold.groupBy(id_column)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        invalid_target = 0

        if target_col == "ph":
            invalid_target = df_gold.filter((F.col("ph") < 0) | (F.col("ph") > 14)).count()

        elif target_col == "pct_c":
            invalid_target = df_gold.filter(F.col("pct_c") < 0).count()

        elif target_col == "olsen_p_mg_kg":
            invalid_target = df_gold.filter(F.col("olsen_p_mg_kg") < 0).count()

        # elif target_col == "ecec_cmolc__kg_soil":
        #    invalid_target = df_gold.filter(F.col("ecec_cmolc__kg_soil") < 0).count()

        elif target_col == "exbas":
            invalid_target = df_gold.filter(F.col("exbas") < 0).count()

        temp_path = f"{GOLD_BASE_PATH}{gold_name}/_tmp/"
        final_path = f"{GOLD_BASE_PATH}{gold_name}/gold_{gold_name}_model_ready.parquet"

        temp_bucket, temp_prefix = parse_s3_uri(temp_path)
        final_bucket, final_key = parse_s3_uri(final_path)

        delete_prefix(temp_bucket, temp_prefix)
        delete_prefix(final_bucket, final_key)

        (
            df_gold
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(temp_path)
        )

        move_single_parquet_from_temp(temp_path, final_path)

        final_size = get_object_size(final_bucket, final_key)

        if output_rows == 0:
            status = "empty_gold_after_dropna"

        elif duplicate_ssn > 0:
            status = "success_with_duplicates"

        elif invalid_target > 0:
            status = "success_with_invalid_target"

    except Exception as e:
        status = "failed"
        error_message = str(e)

        input_rows = 0
        output_rows = 0
        dropped_rows = 0
        pct_dropped = 0
        null_target_rows = 0
        duplicate_ssn = 0
        invalid_target = 0
        final_size = 0
        final_path = ""

    audit_rows.append({
        "execution_utc": execution_utc,
        "gold_name": gold_name,
        "target_col": target_col,
        "input_rows": input_rows,
        "output_rows": output_rows,
        "dropped_rows": dropped_rows,
        "pct_rows_dropped": pct_dropped,
        "null_target_rows": null_target_rows,
        "duplicate_ssn": duplicate_ssn,
        "invalid_target_rows": invalid_target,
        "spectral_cols": len(spectral_cols),
        "final_path": final_path,
        "final_file_size": final_size,
        "status": status,
        "error_message": error_message
    })



# Execução
for gold_name, target_col in targets.items():
    build_and_save_gold(df_silver, gold_name, target_col)


# Salvar validação
def salvar_validacoes():

    timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M")
    file_name = f"validations_gold_{timestamp}.csv"
    key = f"{VALIDATIONS_PREFIX}{file_name}"

    fieldnames = list(audit_rows[0].keys()) if audit_rows else []

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(audit_rows)

    bucket, _ = parse_s3_uri(GOLD_BASE_PATH)

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue().encode("utf-8")
    )

    print(f"Validação salva em: s3://{bucket}/{key}")

salvar_validacoes()

job.commit()
print("Job finalizado com sucesso.")