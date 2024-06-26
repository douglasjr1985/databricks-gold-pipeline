import argparse
from loguru import logger
from pyspark.sql import SparkSession

def parse_arguments():
    parser = argparse.ArgumentParser(description='Execute a SQL query and save the result to a table.')
    parser.add_argument('--sql_query', type=str, required=True, help='SQL query to execute')
    parser.add_argument('--database', type=str, required=True, help='Database name')
    parser.add_argument('--table_name', type=str, required=True, help='Table name')
    parser.add_argument('--mode', type=str, default='append', help='Save mode (default: append)')
    parser.add_argument('--primary_key', type=str, help='Primary key for merge operation')
    return parser.parse_args()

def main():
    args = parse_arguments()

    sql_query = args.sql_query
    database = args.database
    table_name = args.table_name
    mode = args.mode
    primary_key = args.primary_key

    # Log the received parameters
    logger.info(f"Executing SQL query: {sql_query}")
    logger.info(f"Saving to database: {database}, table: {table_name}, mode: {mode}")
    if primary_key:
        logger.info(f"Using primary key for merge: {primary_key}")

    # Execute the SQL query
    spark = SparkSession.builder.appName("SQLExecutor").getOrCreate()
    result = spark.sql(sql_query)

    # Show the result
    result.show()

    # Construct the full table name
    full_table_name = f"{database}.{table_name}"

    # Save the table according to the configurations
    if mode == 'merge' and primary_key:
        target_table = spark.table(full_table_name)
        target_table.alias("target").merge(
            result.alias("source"),
            f"target.{primary_key} = source.{primary_key}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.info(f"Table {full_table_name} merged with primary key {primary_key}")
    else:
        result.write.mode(mode).saveAsTable(full_table_name)
        logger.info(f"Table {full_table_name} saved with mode {mode}")

if __name__ == "__main__":
    main()
