import pandas as pd

# Caminho do arquivo que seu pipeline gerou
path = "saida_parquet\.spark-staging-a7537ea5-9956-4cc2-9f3c-b72d01671746"

# Lê o arquivo Parquet
df = pd.read_parquet(path)

# Mostra as 10 primeiras linhas para você conferir
print("Visualizando os dados processados:")
print(df.head(100))

# Mostra informações sobre as colunas e tipos
print("\nEstrutura dos dados:")
print(df.info())