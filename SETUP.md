# Guia de Configuração do Ambiente de Engenharia de Dados (Local)

**Projeto:** Case Técnico - Pipeline ETL (Super IAM)  
**Candidato:** Igor Marcos da Silva

**Objetivo:** Replicar o ambiente de desenvolvimento local (VS Code) numa máquina corporativa, garantindo total compatibilidade com o PySpark e a execução do arquivo oficial do case.

---

## 1. Pré-requisitos de Infraestrutura
Em um ambiente corporativo, garanta que as seguintes ferramentas estejam instaladas (caso não estejam, utilize o portal de software da empresa ou abra um chamado de suporte):

* **Java 11 (OpenJDK 11):** É OBRIGATÓRIO utilizar a versão 11 para garantir a compatibilidade com o AWS Glue 4.0. Versões superiores (como o Java 17) podem causar erros de alocação de memória no PySpark. Verifique se a variável de ambiente JAVA_HOME está devidamente configurada.
* **Python (3.9 ou 3.10):** Evite instalar a versão 3.12 na máquina corporativa para não ter conflitos de bibliotecas descontinuadas (como o distutils). As versões 3.9 e 3.10 são nativamente estáveis com o Spark 3.3.
* **VS Code (Extensões):** Instale as extensões oficiais da Microsoft (Python e Jupyter) para conseguir abrir e executar o arquivo .ipynb corretamente.

---

## 2. Preparação do Ambiente Virtual (Terminal do VS Code)
Abra a pasta do projeto no VS Code, abra o terminal integrado (Ctrl + ') e execute os comandos abaixo para isolar as bibliotecas do projeto:

**Passo 2.1:** Criar o ambiente virtual:
python -m venv .venv

**Passo 2.2:** Ativar o ambiente:
(Se ocorrer erro de permissão no Windows corporativo, execute primeiro: Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser)
.\.venv\Scripts\activate

**Passo 2.3:** Instalar as bibliotecas na versão exata do edital (Glue 4.0):
pip install pyspark==3.3.2 pandas pyarrow setuptools

---

## 3. Integração com a Base de Dados Original (Arquivo Parquet)
Para que o código processe a base oficial em vez da base fictícia de testes, realize os seguintes ajustes:

1. Baixe o arquivo real enviado pela equipe de recrutamento (nomeado no PDF como amostrasuperiam.parquet).
2. Coloque este arquivo EXATAMENTE NA MESMA PASTA onde se encontra o seu pipeline_etl.ipynb.
3. Abra o notebook no VS Code, vá até a Célula de Configuração e Leitura dos Dados e altere o nome da variável input_path para o nome exato do arquivo real:

Exemplo de como deve ficar no código:
input_path = "amostrasuperiam.parquet"
df_raw = spark.read.parquet(input_path)

---

## 4. Pontos de Atenção Durante a Execução
Ao executar o notebook com os dados reais perante os avaliadores, dois comportamentos normais irão ocorrer. Use-os a seu favor na defesa técnica:

* **Aviso do Winutils / Hadoop:** Como a máquina corporativa opera em Windows, o PySpark emitirá um aviso de que não encontrou o winutils.exe ao tentar gravar. Não há problema. O código possui um mecanismo de contingência (fallback) implementado na última célula que capta os dados em memória e grava o arquivo em formato Parquet através do Pandas, garantindo a entrega do resultado final sem falhas.
* **Data Quality Gates (Alertas de Qualidade):** Não sabemos qual é o nível de sujeira da base real. Se os Gates de Qualidade de Dados (como os validadores de CNPJ e de datas nulas) falharem de forma mais agressiva na tela, utilize isso como argumento. Demonstre que o pipeline é resiliente e foi construído exatamente para alertar a equipe sobre anomalias críticas antes que estas contaminem a camada Refined.