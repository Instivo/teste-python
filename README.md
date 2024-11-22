# Teste Python/ETL
## Apache Airflow DAG: PostgreSQL to MongoDB Workflow

## Descrição

Este projeto implementa uma DAG no Apache Airflow para realizar o seguinte fluxo de trabalho:

1. **Leitura de Dados no PostgreSQL**  
   Conecta-se ao banco de dados PostgreSQL e realiza a leitura da tabela `entrega`, extraindo os campos `id` e `data_entrega`.

2. **Cálculo de Diferença de Tempo**  
   Para cada registro, calcula a diferença em minutos entre a data/hora atual e o campo `data_entrega`.

3. **Inserção no MongoDB**  
   Insere os resultados em uma coleção chamada `timeDiff` no MongoDB, adicionando os seguintes campos ao documento:
   - `id_entrega`: ID da entrega.
   - `data_entrega`: Data de entrega original.
   - `time_diff`: Diferença de tempo calculada (em minutos).
   - `cor`: Classificação baseada no valor de `time_diff`:
     - **`vermelho`**: Se `time_diff > 50`.
     - **`amarelo`**: Se `30 <= time_diff <= 50`.
     - **`verde`**: Se `time_diff < 30`.

## Exemplo de Documento no MongoDB

```json
{
  "id_entrega": 1,
  "data_entrega": "2024-11-21T12:00:00",
  "time_diff": 45.0,
  "cor": "amarelo"
}