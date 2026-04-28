# Arquitetura do Projeto

O projeto segue uma arquitetura em camadas para organizar a ingestão, tratamento e preparação dos dados para modelagem.

## Camadas

### Bronze

Camada bruta, responsável por armazenar os arquivos exatamente como foram obtidos do bucket público AfSIS.

### Silver

Camada tratada e consolidada. Nessa etapa, os espectros são extraídos dos arquivos OPUS e combinados com as bases Wet Chemistry por meio da chave `SSN`.

### Gold

Camada final para modelagem. São criadas cinco bases, uma para cada target, removendo registros nulos e mantendo somente as features espectrais necessárias.

## Fluxo

```text
AfSIS público → Bronze → Silver master → Gold por target → Modelagem
```

## Componentes AWS

- Amazon S3 para armazenamento das camadas.
- AWS Glue para execução dos jobs PySpark/Python.
- Notebook Python para modelagem.
