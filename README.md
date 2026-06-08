# Case Técnico: Engenharia de Dados - Itaú Unibanco

Projeto de um pipeline ETL desenvolvido em PySpark para processamento da tabela `super_iam`.

## Funcionalidades
- **Ingestão:** Leitura de base bruta em Parquet.
- **Qualidade:** Implementação de Data Quality Gates para monitoramento de inconsistências.
- **Transformação:** Aplicação de 5 regras de negócio (Faixa de tempo, Score de Maturidade, Risco, Concentração e Flag Multicontas).
- **Observabilidade:** Geração de métricas de execução e controle de volumetria.

## Como rodar
1. Configure o ambiente: `pip install pyspark pandas pyarrow`
2. Execute o notebook: `pipeline_etl.ipynb`

## Documentação de Setup
Consulte o arquivo `SETUP.md` para detalhes sobre a configuração do ambiente Java/Python.
