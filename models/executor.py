import sys
import json
from pyspark.sql import SparkSession

# Pega a query SQL e o caminho da configuração passados como argumentos
sql_query = sys.argv[1]
config_path = sys.argv[2]

# Log da consulta SQL e do caminho da configuração recebidos
print(f"Executing SQL query: {sql_query}")
print(f"Loading config from: {config_path}")

# Executa a consulta SQL
spark = SparkSession.builder.appName("SQLExecutor").getOrCreate()
result = spark.sql(sql_query)

# Mostra o resultado
result.show()

# Carrega as configurações de escrita
with open(config_path, 'r') as f:
    config = json.load(f)

database = config.get("database")
table_name = config.get("table_name")
mode = config.get("mode", "append")  # Default para append se não especificado

# Constrói o nome completo da tabela
full_table_name = f"{database}.{table_name}"

# Salva a tabela conforme as configurações
result.write.mode(mode).saveAsTable(full_table_name)
print(f"Table {full_table_name} saved with mode {mode}")
