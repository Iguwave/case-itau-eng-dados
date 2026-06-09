import pandas as pd

# Define para o Pandas não cortar as colunas na hora de printar na tela
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

print("=" * 60)
print("1. LENDO OS DADOS REFINADOS (SUPER_IAM)")
print("=" * 60)

# Carrega o arquivo final gerado na última execução
df_refined = pd.read_parquet("saida_parquet_final.parquet")

# Mostra a quantidade de linhas e colunas salvas
print(f"Total de registros processados: {df_refined.shape[0]}")
print(f"Total de colunas no Schema Refined: {df_refined.shape[1]}")

print("\nAmostra dos primeiros 5 registros com as novas colunas de negócio:")
# Selecionamos algumas colunas principais + as regras que você criou para ficar fácil de ver
colunas_interesse = [
    "cnpj14", "cod_operador", "risco_operador", 
    "score_maturidade_digital", "classificacao_digital", 
    "concentracao_operadores", "flag_operador_multicontas"
]
print(df_refined[colunas_interesse].head(5))


print("\n" + "=" * 60)
print("2. LENDO AS MÉTRICAS DE OBSERVABILIDADE PERSISTIDAS")
print("=" * 60)

# Carrega o arquivo de métricas
df_metrics = pd.read_parquet("saida_metrics_final.parquet")
print(df_metrics.to_string(index=False))