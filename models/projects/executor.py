import sys
from pyspark.sql import SparkSession

# Pega a query SQL passada como argumento
sql_query = sys.argv[1]

# Log da consulta SQL recebida
print(f"Executing SQL query: {sql_query}")

# Executa a consulta SQL
spark = SparkSession.builder.appName("SQLExecutor").getOrCreate()
result = spark.sql(sql_query)

# Mostra o resultado
result.show()
