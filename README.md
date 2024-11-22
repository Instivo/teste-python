Crie uma DAG no Apache Airflow que execute o seguinte fluxo de trabalho:

Leitura de dados no PostgreSQL

Conecte-se ao banco de dados PostgreSQL e faça a leitura da tabela entrega.
Extraia os campos id e data_entrega.
Cálculo de diferença de tempo

Para cada registro, calcule a diferença em minutos entre a data/hora atual e o campo data_entrega.
Inserção no MongoDB

Insira os resultados em uma coleção chamada timeDiff no MongoDB.

Adicione os seguintes campos ao documento:
id_entrega: ID da entrega.
data_entrega: Data de entrega original.
time_diff: Diferença de tempo calculada (em minutos).
cor: Uma classificação baseada no valor de time_diff, conforme as regras abaixo:
Se time_diff > 50: "vermelho".
Se 30 <= time_diff <= 50: "amarelo".
Se time_diff < 30: "verde".


Exemplo de documento esperado no MongoDB:

{
  "id_entrega": 1,
  "data_entrega": "2024-11-21T12:00:00",
  "time_diff": 45.0,
  "cor": "amarelo"
}

Certifique-se de implementar conexões seguras para os bancos de dados e de tratar possíveis erros, 
como falhas de conexão ou dados inconsistentes.

A DAG deve ser configurada para ser executada periodicamente, conforme a necessidade.
