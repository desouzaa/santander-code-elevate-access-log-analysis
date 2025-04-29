# Databricks notebook source
# MAGIC %md
# MAGIC ###Include ETL

# COMMAND ----------

# MAGIC %run ./etl_pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ###Parametrização

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Exemplo de parâmetro onde é extraído o dado da URL

# COMMAND ----------

#O intuito é que o pipeline funcione de forma genérica para diversas regras, padronizando toda a ingestão e processo de transformação, onde o engenheiro se preocupa apenas em definir a regra que será utilziada para gerar a silver e a gold. Se o is_log estiver definido como True, a regra sql silver não se faz necessária pois como é um log padrão, já há um tratamento para gerar a tabela corretamente.

sql_regra_gold = """
            
            WITH base AS (
                SELECT 
                    client_ip,
                    endpoint_clean,
                    response_size,
                    status_code,
                    to_date(timestamp) AS date,
                    date_format(timestamp, 'EEEE') AS day_of_week,
                    CASE 
                        WHEN status_code BETWEEN 100 AND 199 THEN 'Informational'
                        WHEN status_code BETWEEN 200 AND 299 THEN 'Success'
                        WHEN status_code BETWEEN 300 AND 399 THEN 'Redirection'
                        WHEN status_code BETWEEN 400 AND 499 THEN 'Client Error'
                        WHEN status_code BETWEEN 500 AND 599 THEN 'Server Error'
                        ELSE 'Unknown'
                    END AS response_category
                FROM silver.s_access_logs
            ),

            --CTE que traz o top 10 client ip mais acessos
            top_ips AS (
                SELECT COLLECT_LIST(
                        NAMED_STRUCT(
                            'client_ip', client_ip,
                            'access_count', access_count
                        )
                    ) AS top_ips_list
                FROM (
                    SELECT client_ip, COUNT(*) AS access_count
                    FROM base
                    GROUP BY client_ip
                    ORDER BY access_count DESC
                    LIMIT 10
                ) tmp
            ),

            --CTE que traz o top 6 endpoints mais acessados
            top_endpoints AS (
                SELECT COLLECT_LIST(
                        NAMED_STRUCT(
                            'endpoint', endpoint_clean,
                            'access_count', access_count
                        )
                    ) AS top_endpoints_list
                FROM (
                    SELECT endpoint_clean, COUNT(*) AS access_count
                    FROM base
                    WHERE NOT lower(endpoint_clean) RLIKE '\\\\.(css|js|png|jpg|jpeg|gif|txt|ico|svg|woff|ttf|mp4|avi|mov|mpeg|mpg|webm|mkv|bmp)(\\\\?|#|$)'
                    AND endpoint_clean != '/robots.txt'
                    GROUP BY endpoint_clean
                    ORDER BY access_count DESC
                    LIMIT 6
                ) tmp
            ),


            --CTE que traz os dados dos volumes de dados
            volume_stats AS (
                SELECT 
                    SUM(TRY_CAST(response_size AS DOUBLE)) AS total_volume,
                    MAX(TRY_CAST(response_size AS DOUBLE)) AS max_volume,
                    MIN(TRY_CAST(response_size AS DOUBLE)) AS min_volume,
                    AVG(TRY_CAST(response_size AS DOUBLE)) AS avg_volume
                FROM base
            ),

            --CTE que traz os dados de dia da semana com maiores erros
            error_by_day_of_week AS (
                SELECT 
                    day_of_week, COUNT(*) AS client_error_count
                FROM base
                WHERE status_code BETWEEN 400 AND 499
                GROUP BY day_of_week
                ORDER BY client_error_count DESC
                LIMIT 1
            )

            -- Resultado final principal
            SELECT
                (SELECT top_ips_list FROM top_ips) AS top_ips,
                (SELECT top_endpoints_list FROM top_endpoints) AS top_endpoints,
                (SELECT COUNT(DISTINCT client_ip) FROM base) AS distinct_ip_count,
                (SELECT COUNT(DISTINCT date) FROM base) AS distinct_day_count,
                (SELECT day_of_week FROM error_by_day_of_week) AS day_with_most_client_errors,
                volume_stats.total_volume,
                volume_stats.max_volume,
                volume_stats.min_volume,
                volume_stats.avg_volume
            FROM volume_stats
            ;

"""


#Definir parâmetros para rodar o ETL    EXEMPLO URL LOG
params = {
    "pipeline_name": "access_log",
    "url": "https://codeelevatestoragelog.blob.core.windows.net/logs/access_log.txt",
    "table_name_bronze": "b_access_logs",
    "table_name_silver": "s_access_logs",
    "table_name_gold": "g_access_logs",
    "file_name": "access_log.txt",
    "source": "URL",
    "is_log": True,
    "sql_silver": "",
    "sql_gold": sql_regra_gold
}


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ##### Exemplo de parâmetro onde é extraído o dado do Upload feito no DBFS

# COMMAND ----------

#O intuito é que o pipeline funcione de forma genérica para diversas regras, padronizando toda a ingestão e processo de transformação, onde o engenheiro se preocupa apenas em definir a regra que será utilziada para gerar a silver e a gold. Se o is_log estiver definido como True, a regra sql silver não se faz necessária pois como é um log padrão, já há um tratamento para gerar a tabela corretamente.

sql_regra_gold = """
            
            WITH base AS (
                SELECT 
                    client_ip,
                    endpoint_clean,
                    response_size,
                    status_code,
                    to_date(timestamp) AS date,
                    date_format(timestamp, 'EEEE') AS day_of_week,
                    CASE 
                        WHEN status_code BETWEEN 100 AND 199 THEN 'Informational'
                        WHEN status_code BETWEEN 200 AND 299 THEN 'Success'
                        WHEN status_code BETWEEN 300 AND 399 THEN 'Redirection'
                        WHEN status_code BETWEEN 400 AND 499 THEN 'Client Error'
                        WHEN status_code BETWEEN 500 AND 599 THEN 'Server Error'
                        ELSE 'Unknown'
                    END AS response_category
                FROM silver.s_access_logs
            ),

            --CTE que traz o top 10 client ip mais acessos
            top_ips AS (
                SELECT COLLECT_LIST(
                        NAMED_STRUCT(
                            'client_ip', client_ip,
                            'access_count', access_count
                        )
                    ) AS top_ips_list
                FROM (
                    SELECT client_ip, COUNT(*) AS access_count
                    FROM base
                    GROUP BY client_ip
                    ORDER BY access_count DESC
                    LIMIT 10
                ) tmp
            ),

            --CTE que traz o top 6 endpoints mais acessados
            top_endpoints AS (
                SELECT COLLECT_LIST(
                        NAMED_STRUCT(
                            'endpoint', endpoint_clean,
                            'access_count', access_count
                        )
                    ) AS top_endpoints_list
                FROM (
                    SELECT endpoint_clean, COUNT(*) AS access_count
                    FROM base
                    WHERE NOT lower(endpoint_clean) RLIKE '\\\\.(css|js|png|jpg|jpeg|gif|txt|ico|svg|woff|ttf|mp4|avi|mov|mpeg|mpg|webm|mkv|bmp)(\\\\?|#|$)'
                    AND endpoint_clean != '/robots.txt'
                    GROUP BY endpoint_clean
                    ORDER BY access_count DESC
                    LIMIT 6
                ) tmp
            ),


            --CTE que traz os dados dos volumes de dados
            volume_stats AS (
                SELECT 
                    SUM(TRY_CAST(response_size AS DOUBLE)) AS total_volume,
                    MAX(TRY_CAST(response_size AS DOUBLE)) AS max_volume,
                    MIN(TRY_CAST(response_size AS DOUBLE)) AS min_volume,
                    AVG(TRY_CAST(response_size AS DOUBLE)) AS avg_volume
                FROM base
            ),

            --CTE que traz os dados de dia da semana com maiores erros
            error_by_day_of_week AS (
                SELECT 
                    day_of_week, COUNT(*) AS client_error_count
                FROM base
                WHERE status_code BETWEEN 400 AND 499
                GROUP BY day_of_week
                ORDER BY client_error_count DESC
                LIMIT 1
            )

            -- Resultado final principal
            SELECT
                (SELECT top_ips_list FROM top_ips) AS top_ips,
                (SELECT top_endpoints_list FROM top_endpoints) AS top_endpoints,
                (SELECT COUNT(DISTINCT client_ip) FROM base) AS distinct_ip_count,
                (SELECT COUNT(DISTINCT date) FROM base) AS distinct_day_count,
                (SELECT day_of_week FROM error_by_day_of_week) AS day_with_most_client_errors,
                volume_stats.total_volume,
                volume_stats.max_volume,
                volume_stats.min_volume,
                volume_stats.avg_volume
            FROM volume_stats
            ;

"""


#Definir parâmetros para rodar o ETL  EXEMPLO UPLOADED LOG
params = {
    "pipeline_name": "access_log_upload",
    "url": "https://codeelevatestoragelog.blob.core.windows.net/logs/access_log.txt",
    "table_name_bronze": "b_access_logs_upload",
    "table_name_silver": "s_access_logs_upload",
    "table_name_gold": "g_access_logs_upload",
    "file_name": "access_log.txt",
    "source": "UPLOADED",
    "is_log": True,
    "sql_silver": "",
    "sql_gold": sql_regra_gold
}


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Instância da classe ETLPipeline

# COMMAND ----------

etl = ETLPipeline(spark, **params)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Execução do pipeline completo

# COMMAND ----------

etl.execute_etl(4)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Execução por etapas do pipeline

# COMMAND ----------

etl.source_to_landing()

# COMMAND ----------

etl.landing_to_bronze()

# COMMAND ----------

etl.bronze_to_silver()

# COMMAND ----------

etl.silver_to_gold()