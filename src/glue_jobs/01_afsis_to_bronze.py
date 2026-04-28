import os
import csv
import io
import tempfile
from datetime import datetime

import boto3
from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import ClientError


SOURCE_BUCKET = "afsis"
DEST_BUCKET = "projeto-edb-015"
DEST_PREFIX = "bronze/afsis/"
VALIDATIONS_PREFIX = "validations/bronze/afsis/"
AWS_REGION = "us-east-1"

PREFIXES = [
    "2009-2013/Dry_Chemistry/ICRAF/Bruker_Alpha_KBr/",
    "2009-2013/Wet_Chemistry/CROPNUTS/",
    "2009-2013/Wet_Chemistry/ICRAF/",
    "2009-2013/Wet_Chemistry/RRES/",
]


def build_public_client():
    return boto3.client(
        "s3",
        config=Config(signature_version=UNSIGNED),
        region_name=AWS_REGION
    )


def build_private_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION
    )


def list_source_objects(s3_public, bucket, prefix):
    paginator = s3_public.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.endswith("/"):
                continue

            yield {
                "Key": key,
                "Size": obj["Size"]
            }


def object_exists(s3_private, bucket, key):
    try:
        s3_private.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")

        if error_code in ("404", "NoSuchKey", "NotFound"):
            return False

        raise


def get_object_size(s3_private, bucket, key):
    response = s3_private.head_object(Bucket=bucket, Key=key)
    return response["ContentLength"]


def copy_object_via_tempfile(
    s3_public,
    s3_private,
    source_bucket,
    source_key,
    dest_bucket,
    dest_key
):
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        temp_path = tmp.name

    try:
        s3_public.download_file(source_bucket, source_key, temp_path)
        s3_private.upload_file(temp_path, dest_bucket, dest_key)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def salvar_validacoes_csv(s3_private, audit_rows):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M")
    file_name = f"validations_bronze_{timestamp}.csv"
    s3_key = f"{VALIDATIONS_PREFIX}{file_name}"

    fieldnames = [
        "execution_utc",
        "prefix",
        "source_bucket",
        "source_key",
        "dest_bucket",
        "dest_key",
        "source_size",
        "dest_size",
        "status",
        "error_message"
    ]

    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(audit_rows)

    s3_private.put_object(
        Bucket=DEST_BUCKET,
        Key=s3_key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv"
    )

    print(f"Arquivo de validação salvo em: s3://{DEST_BUCKET}/{s3_key}")


def main():
    print("JOB INICIOU")
    print(f"Origem: s3://{SOURCE_BUCKET}/")
    print(f"Destino base: s3://{DEST_BUCKET}/{DEST_PREFIX}")
    print(f"Validações: s3://{DEST_BUCKET}/{VALIDATIONS_PREFIX}")
    print(f"Total de prefixes configurados: {len(PREFIXES)}")

    s3_public = build_public_client()
    s3_private = build_private_client()

    total_found = 0
    total_copied = 0
    total_skipped = 0
    total_failed = 0
    total_zero_byte = 0
    total_size_mismatch = 0

    audit_rows = []

    execution_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for p_idx, prefix in enumerate(PREFIXES, start=1):
        print("=" * 100)
        print(f"[PREFIX {p_idx}/{len(PREFIXES)}] Iniciando prefix: {prefix}")

        objects = list(list_source_objects(s3_public, SOURCE_BUCKET, prefix))
        prefix_total = len(objects)
        total_found += prefix_total

        print(f"[PREFIX {p_idx}/{len(PREFIXES)}] Arquivos encontrados: {prefix_total}")

        if prefix_total == 0:
            print(f"[PREFIX {p_idx}/{len(PREFIXES)}] Nenhum arquivo encontrado para este prefix.")
            audit_rows.append({
                "execution_utc": execution_utc,
                "prefix": prefix,
                "source_bucket": SOURCE_BUCKET,
                "source_key": "",
                "dest_bucket": DEST_BUCKET,
                "dest_key": "",
                "source_size": "",
                "dest_size": "",
                "status": "empty_prefix",
                "error_message": "Nenhum arquivo encontrado para o prefix."
            })
            continue

        prefix_copied = 0
        prefix_skipped = 0
        prefix_failed = 0
        prefix_zero_byte = 0
        prefix_size_mismatch = 0

        for i, obj in enumerate(objects, start=1):
            source_key = obj["Key"]
            source_size = obj["Size"]
            dest_key = f"{DEST_PREFIX}{source_key}"

            try:
                if source_size == 0:
                    prefix_zero_byte += 1
                    total_zero_byte += 1
                    print(f"[PREFIX {p_idx}/{len(PREFIXES)}] [{i}/{prefix_total}] ZERO_BYTE {source_key}")

                if object_exists(s3_private, DEST_BUCKET, dest_key):
                    prefix_skipped += 1
                    total_skipped += 1

                    try:
                        dest_size = get_object_size(s3_private, DEST_BUCKET, dest_key)
                    except Exception:
                        dest_size = ""

                    audit_rows.append({
                        "execution_utc": execution_utc,
                        "prefix": prefix,
                        "source_bucket": SOURCE_BUCKET,
                        "source_key": source_key,
                        "dest_bucket": DEST_BUCKET,
                        "dest_key": dest_key,
                        "source_size": source_size,
                        "dest_size": dest_size,
                        "status": "skipped_existing",
                        "error_message": ""
                    })

                    print(f"[PREFIX {p_idx}/{len(PREFIXES)}] [{i}/{prefix_total}] SKIP {source_key}")
                    continue

                print(f"[PREFIX {p_idx}/{len(PREFIXES)}] [{i}/{prefix_total}] COPIANDO {source_key}")

                copy_object_via_tempfile(
                    s3_public=s3_public,
                    s3_private=s3_private,
                    source_bucket=SOURCE_BUCKET,
                    source_key=source_key,
                    dest_bucket=DEST_BUCKET,
                    dest_key=dest_key
                )

                if not object_exists(s3_private, DEST_BUCKET, dest_key):
                    raise FileNotFoundError(f"Objeto não encontrado no destino após upload: {dest_key}")

                dest_size = get_object_size(s3_private, DEST_BUCKET, dest_key)

                if dest_size != source_size:
                    prefix_size_mismatch += 1
                    total_size_mismatch += 1

                    audit_rows.append({
                        "execution_utc": execution_utc,
                        "prefix": prefix,
                        "source_bucket": SOURCE_BUCKET,
                        "source_key": source_key,
                        "dest_bucket": DEST_BUCKET,
                        "dest_key": dest_key,
                        "source_size": source_size,
                        "dest_size": dest_size,
                        "status": "copied_with_size_mismatch",
                        "error_message": "Tamanho no destino diferente do tamanho da origem."
                    })

                    print(
                        f"[PREFIX {p_idx}/{len(PREFIXES)}] [{i}/{prefix_total}] "
                        f"SIZE_MISMATCH {source_key} | origem={source_size} destino={dest_size}"
                    )
                else:
                    audit_rows.append({
                        "execution_utc": execution_utc,
                        "prefix": prefix,
                        "source_bucket": SOURCE_BUCKET,
                        "source_key": source_key,
                        "dest_bucket": DEST_BUCKET,
                        "dest_key": dest_key,
                        "source_size": source_size,
                        "dest_size": dest_size,
                        "status": "copied",
                        "error_message": ""
                    })

                    print(f"[PREFIX {p_idx}/{len(PREFIXES)}] [{i}/{prefix_total}] OK {source_key}")

                prefix_copied += 1
                total_copied += 1

            except Exception as e:
                prefix_failed += 1
                total_failed += 1

                audit_rows.append({
                    "execution_utc": execution_utc,
                    "prefix": prefix,
                    "source_bucket": SOURCE_BUCKET,
                    "source_key": source_key,
                    "dest_bucket": DEST_BUCKET,
                    "dest_key": dest_key,
                    "source_size": source_size,
                    "dest_size": "",
                    "status": "failed",
                    "error_message": str(e)
                })

                print(f"[PREFIX {p_idx}/{len(PREFIXES)}] [{i}/{prefix_total}] ERROR {source_key} -> {str(e)}")

        print(f"[PREFIX {p_idx}/{len(PREFIXES)}] Finalizado")
        print(f"[PREFIX {p_idx}/{len(PREFIXES)}] Copiados: {prefix_copied}")
        print(f"[PREFIX {p_idx}/{len(PREFIXES)}] Pulados: {prefix_skipped}")
        print(f"[PREFIX {p_idx}/{len(PREFIXES)}] Falhas: {prefix_failed}")
        print(f"[PREFIX {p_idx}/{len(PREFIXES)}] Zero byte: {prefix_zero_byte}")
        print(f"[PREFIX {p_idx}/{len(PREFIXES)}] Size mismatch: {prefix_size_mismatch}")

    salvar_validacoes_csv(s3_private, audit_rows)

    print("=" * 100)
    print("JOB FINALIZADO")
    print(f"Total encontrados: {total_found}")
    print(f"Total copiados: {total_copied}")
    print(f"Total pulados: {total_skipped}")
    print(f"Total falhas: {total_failed}")
    print(f"Total zero byte: {total_zero_byte}")
    print(f"Total size mismatch: {total_size_mismatch}")


if __name__ == "__main__":
    main()