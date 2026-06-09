import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

n_rows = 5000

def random_date(start, end):
    return start + timedelta(days=random.randint(0, int((end - start).days)))

start_dt = datetime(2010, 1, 1)
end_dt = datetime(2023, 12, 31)

data = {
    "cnpj14": [str(random.randint(10000000000000, 99999999999999)) if random.random() > 0.05 else None for _ in range(n_rows)],
    "grupo_segmento": np.random.choice(["VAREJO", "ATACADO", "PRIVATE"], n_rows),
    "segmento_detalhado": np.random.choice(["Varejo PJ", "Atacado Middle", "null"], n_rows),
    "modelo_atendimento": np.random.choice(["DIGITAL", "AGENCIA", " "], n_rows),
    "situacao_conta": np.random.choice(["ATIVA", "ENCERRADA", "BLOQUEADA"], n_rows),
    "situacao_cadastral_receita": np.random.choice(["ATIVA", "INAPTA"], n_rows),
    "data_abertura_conta": [random_date(start_dt, end_dt).strftime("%Y-%m-%d") for _ in range(n_rows)],
    "faixa_tempo_criacao_conta": np.random.choice(["Ate 1 ano", "1 a 3 anos", "Mais de 3 anos"], n_rows),
    "data_fundacao_empresa": [random_date(datetime(1990,1,1), end_dt).strftime("%Y-%m-%d") for _ in range(n_rows)],
    "faixa_tempo_vida_empresa": np.random.choice(["Ate 5 anos", "Mais de 5 anos"], n_rows),
    "data_inicio_relacionamento_banco": [random_date(start_dt, end_dt).strftime("%Y-%m-%d") for _ in range(n_rows)],
    "uf_sigla": np.random.choice(["SP", "RJ", "MG", "PR", "null"], n_rows),
    "tipo_sociedade": np.random.choice(["LTDA", "SA", "MEI"], n_rows),
    "quantidade_socios": np.random.choice(["1.0", "2", "003", "null"], n_rows),
    "cod_operador": np.random.choice(["1001.0", "1002", "1003", "001004"], n_rows),
    "tipo_operador": np.random.choice(["operador", "representante legal", "procurador"], n_rows),
    "data_criacao_operador": [random_date(start_dt, end_dt).strftime("%Y-%m-%d") for _ in range(n_rows)],
    "faixa_tempo_criacao_operador": np.random.choice(["Ate 1 ano", "Mais de 1 ano"], n_rows),
    "sit_operador": np.random.choice(["ativo", "inativo", "bloqueado", " "], n_rows),
    "mot_blog_operador": np.random.choice(["Fraude", "Inatividade", "null"], n_rows),
    "flag_firmas_e_poderes": np.random.choice(["S", "N", "Sim", "Não", "1", "0"], n_rows),
    "qtd_operadores_associados_ao_cnpj": np.random.choice(["1", "3", "12.0", "null"], n_rows),
    "qtd_contas_associadas_ao_operador": np.random.choice(["1", "2", "15", "005"], n_rows),
    "tem_token_mobile_habilitado": np.random.choice(["Sim", "Não", "sim", "nao", "True", "False"], n_rows),
    "tem_token_embarcado_habilitado": np.random.choice(["Sim", "Não", "s", "n"], n_rows),
    "tipo_token": np.random.choice(["mobile", "embarcado", "ambos", "null"], n_rows),
    "status_token": np.random.choice(["Habilitado", "Desabilitado"], n_rows),
    "forma_alteracao_token": np.random.choice(["App", "Agencia"], n_rows),
    "ref_token": [random_date(start_dt, end_dt).strftime("%Y-%m-%d") for _ in range(n_rows)],
    "flag_acessou_canal": np.random.choice(["S", "N", "sim", "nao"], n_rows),
    "flag_acessou_mobile": np.random.choice(["S", "N"], n_rows),
    "flag_acessou_web": np.random.choice(["S", "N"], n_rows),
    "flag_acessou_app_comp": np.random.choice(["S", "N"], n_rows),
    "flag_acessou_canal_90d_anteriores": np.random.choice(["S", "N"], n_rows),
    "qtd_total_sessoes": np.random.choice(["0", "5", "15.0", "null"], n_rows),
    "qtd_sessoes_mobile": np.random.choice(["0", "5", "10"], n_rows),
    "qtd_sessoes_web": np.random.choice(["0", "5"], n_rows),
    "ref_anomes": np.random.choice(["2026-05-01", "2026/05/01", "202605", "null"], n_rows)
}

df = pd.DataFrame(data)
df.to_parquet("amostra_super_iam.parquet", index=False)
print("Arquivo amostra_super_iam.parquet gerado com sucesso!")