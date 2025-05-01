# Databricks notebook source
# MAGIC %md
# MAGIC ####Tests

# COMMAND ----------

# MAGIC %run ./etl_pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ####Imports

# COMMAND ----------

from datetime import datetime
import pytest
from pyspark.sql import Row
from zoneinfo import ZoneInfo

# COMMAND ----------

# MAGIC %md
# MAGIC #### Teste unitário para a função validate_quality (Esperado que passe, dados são válidos)

# COMMAND ----------

#Criando um dataframe fake mockado
data = [
    Row(client_ip="192.168.1.1", endpoint="/home", protocol = 'HTTP/1.1', status_code=200, response_size = '1', timestamp=datetime.now()),
    Row(client_ip="10.223.157.2", endpoint="/about", protocol = 'HTTP/1.1',  status_code=400, response_size = '123', timestamp=datetime.now()),
    Row(client_ip="12.213.157.1", endpoint="/banana", protocol = 'HTTP/1.1',  status_code=404, response_size = '123123123123123123123123', timestamp=datetime.now()),
    Row(client_ip="13.223.157.4", endpoint="/pera", protocol = 'HTTP/1.1',  status_code=301, response_size = '0', timestamp=datetime.now()),
    Row(client_ip="14.233.157.5", endpoint="/oi", protocol = 'HTTP/1.1',  status_code=401, response_size = '-', timestamp=datetime.now()),
    Row(client_ip="15.243.157.6", endpoint="/about", protocol = 'HTTP/1.1',  status_code=202, response_size = '234', timestamp=datetime.now()),
    Row(client_ip="16.253.157.7", endpoint="/sobre", protocol = 'HTTP/1.1',  status_code=201, response_size = '223445', timestamp=datetime.now()),
]

#Tranforma em DF Spark
df_valid = spark.createDataFrame(data)

#Instanciar o pipeline
etl = ETLPipeline(spark,
    pipeline_name="test_pipeline",
    url = '',
    file_name="test.txt",
    source="UPLOADED",
    is_log=True,
    table_name_bronze="b_test",
    table_name_silver="s_test",
    table_name_gold="g_test",
    sql_silver="",
    sql_gold=""" SELECT count(*) FROM silver.s_test """
)

#Tentar executar o validate_quality : deve passar sem erro
try:
    etl.validate_quality(df_valid, step="bronze_to_silver", table_name="s_test", is_log=True)
    print("✅ Teste de qualidade com dados válidos passou.")
except Exception as e:
    print(f"❌ Teste falhou inesperadamente: {e}")


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #### Teste unitário para a função validate_quality (Esperado que de erro e não passe, dados são inválidos)

# COMMAND ----------

#Criando um dataframe fake mockado
data = [
    Row(client_ip="192.168.1.1", endpoint="/home", protocol = 'HTTP/1.1', status_code=200, response_size = '12345', timestamp=datetime.now()),
    Row(client_ip=None, endpoint="/home", protocol = 'HTTP/1.1',  status_code=None, response_size = None, timestamp=datetime.now()),
    Row(client_ip='1', endpoint=None, protocol = 'HTTP/1.1',  status_code=None, response_size = None, timestamp=datetime.now()),
    Row(client_ip='2', endpoint="/home", protocol = None,  status_code=None, response_size = None, timestamp=datetime.now()),
    Row(client_ip='3', endpoint="/home", protocol = 'HTTP/1.1',  status_code=None, response_size = None, timestamp=datetime.now()),
]

#Tranforma em DF Spark
df_valid = spark.createDataFrame(data)

#Instanciar o pipeline
etl = ETLPipeline(spark,
    pipeline_name="test_pipeline",
    url = '',
    file_name="test.txt",
    source="UPLOADED",
    is_log=True,
    table_name_bronze="b_test",
    table_name_silver="s_test",
    table_name_gold="g_test",
    sql_silver="",
    sql_gold=""" SELECT count(*) FROM silver.s_test """
)

#Tentar executar o validate_quality — nãodeve passar, está com erro
try:
    etl.validate_quality(df_valid, step="bronze_to_silver", table_name="s_test", is_log=True)
    print("✅Teste de qualidade com dados válidos passou.")
except Exception as e:
    print(f"❌Teste falhou inesperadamente: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Teste de função safe_mv que deve retornar com sucesso

# COMMAND ----------

#Aqui cria um arquivo de teste no path de origem
dbutils.fs.put(f"{etl.path_landing}teste.txt", "Conteúdo de teste", True)

#Instanciar o pipeline
etl = ETLPipeline(spark,
    pipeline_name="test_pipeline",
    url = '',
    file_name="teste.txt",
    source="UPLOADED",
    is_log=True,
    table_name_bronze="b_test",
    table_name_silver="s_test",
    table_name_gold="g_test",
    sql_silver="",
    sql_gold=""" SELECT count(*) FROM silver.s_test """
)

#Verifica se o arquivo existe antes de mover
print("Antes de mover:")
display(dbutils.fs.ls(etl.path_landing))

#Executa o safe_mv
resultado = etl.safe_mv(
    path_source=etl.path_landing,
    path_processed=etl.path_processed,
    file_name="teste.txt"
)

#Verifica se a função retornou True
assert resultado is True, "❌safe_mv deveria ser True"

#Verifica se o arquivo sumiu da origem
print("Após mover, origem:")
try:
    display(dbutils.fs.ls(etl.path_landing))
except Exception as e:
    print("✅Origem limpa.")

#Verifica se o arquivo está na pasta processada (com timestamp)
print("Verificando destino:")
display(dbutils.fs.ls(etl.path_processed))
print("✅Teste finalizado com sucesso.")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Teste de função save_execution_log que deve retornar com sucesso

# COMMAND ----------

#Prepara o ambiente para poder ter certeza que a tabela de logs existe
etl.prepare_environment()

#Pega data hora
now = datetime.now(ZoneInfo("America/Sao_Paulo"))

#Salva log de teste
etl.save_execution_log(
    execution_time=now,
    pipeline_name="test_pipeline",
    step="teste",
    status="OK",
    error_message="",
    processed_file_path="/dbfs/tmp/test_pipe/",
    time_elapsed=3.5
)

# COMMAND ----------

# Verifica se o log foi inserido com sucesso
df_logs = spark.table("monitoring.execution_logs")
df_logs.filter("pipeline_name = 'test_pipeline' AND step = 'teste'").orderBy("execution_time", ascending=False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Teste de função source to landing que deve retornar com sucesso

# COMMAND ----------

#Faz a criação de um  arquivo no FileStore(simula um upload manual no Datbricks)
dbutils.fs.put("dbfs:/FileStore/tables/teste.txt", "conteúdo do arquivo de teste", True)

#Instancia o pipeline com source UPLOADED
etl = ETLPipeline(
    spark=spark,
    pipeline_name="test_pipeline",
    file_name="teste.txt",
    source="UPLOADED",
    is_log=True,
    table_name_bronze="b_test",
    table_name_silver="s_test",
    table_name_gold="g_test",
    sql_silver="",
    sql_gold=""
)

#Executa a função de extração dos dados
etl.source_to_landing()

#Verifica se o arquivo está na pasta landing
print("Arquivos encontrados na pasta landing:")
display(dbutils.fs.ls(etl.path_landing))

#Efetua uma limpeza dos dados inseridos

# Verifica se o log de execução foi salvo corretamente
df_log = spark.sql("SELECT * FROM monitoring.execution_logs WHERE pipeline_name = 'test_pipeline' AND step = 'landing' ORDER BY execution_time DESC")
display(df_log)

#Remover arquivo do FileStore
dbutils.fs.rm("dbfs:/FileStore/tables/teste.txt", True)

#Remover arquivo da landing
dbutils.fs.rm(etl.path_landing, True)

#Remover log de execução
spark.sql("DELETE FROM monitoring.execution_logs WHERE pipeline_name = 'test_pipeline'")



# COMMAND ----------

# MAGIC %md
# MAGIC #### Teste de função source to landing que deve retornar com erro

# COMMAND ----------

#Faz a criação de um  arquivo no FileStore(simula um upload manual no Datbricks)
dbutils.fs.put("dbfs:/FileStore/tables/teste.txt", "conteúdo do arquivo de teste", True)

#Instancia o pipeline com source UPLOADED
etl = ETLPipeline(
    spark=spark,
    pipeline_name="test_pipeline",
    file_name="arquivo_que_nao_existe.txt",
    source="UPLOADED",
    is_log=True,
    table_name_bronze="b_test",
    table_name_silver="s_test",
    table_name_gold="g_test",
    sql_silver="",
    sql_gold=""
)

#Executa a função de extração dos dados e se der erro (ESPERADO) verifica se log foi inserido como NOTOK
try:
    etl.source_to_landing()
except:
    spark.sql("""
        SELECT *
        FROM monitoring.execution_logs
        WHERE pipeline_name LIKE 'test_pipeline' 
        ORDER BY execution_time DESC
        """).display()

#Remover log de execução
spark.sql("DELETE FROM monitoring.execution_logs WHERE pipeline_name = 'test_pipeline'")



# COMMAND ----------

# MAGIC %md
# MAGIC #### Teste de função landing to bronze que deve retornar com sucesso

# COMMAND ----------

#Cria o arquivo de teste na pasta landing
dbutils.fs.put("dbfs:/landing/test_pipeline/teste.txt", "teste", True)

#Instancia o pipeline para esse teste
etl = ETLPipeline(
    spark=spark,
    pipeline_name="test_pipeline",
    file_name="teste.txt",
    source="UPLOADED",
    is_log=True,
    table_name_bronze="b_test",
    table_name_silver="s_test",
    table_name_gold="g_test",
    sql_silver="",
    sql_gold=""
)

#Executa a transformação
etl.landing_to_bronze()

#Verifica se a tabela bronze foi criada e contém os dados esperados
df_bronze = spark.table("bronze.b_test")
display(df_bronze)


#Verifica se o arquivo foi movido da landing para a processed
print("Pasta landing(deve estar vazia sem dados):")
try:
    display(dbutils.fs.ls(etl.path_landing))
except:
    print("Pasta landing vazia.")

print("Pasta processed(deve ter o arquivo):")
display(dbutils.fs.ls(etl.path_processed))

#verifica se o log da execução foi salvo
spark.sql("""
SELECT *
FROM monitoring.execution_logs
WHERE pipeline_name = 'test_pipeline' AND step = 'bronze'
ORDER BY execution_time DESC
""").display()

#Remover log de execução
spark.sql("DELETE FROM monitoring.execution_logs WHERE pipeline_name = 'test_pipeline'")

#Remover arquivo da landing
dbutils.fs.rm(etl.path_landing, True)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Teste de função landing to bronze que deve retornar com erro

# COMMAND ----------

#Cria o arquivo de teste na pasta landing
dbutils.fs.put("dbfs:/landing/test_pipeline/teste.txt", "teste", True)

#Instancia o pipeline para esse teste
etl = ETLPipeline(
    spark=spark,
    pipeline_name="test_pipeline",
    file_name="arquivo_que_nao_existe.txt",
    source="UPLOADED",
    is_log=True,
    table_name_bronze="b_test",
    table_name_silver="s_test",
    table_name_gold="g_test",
    sql_silver="",
    sql_gold=""
)

try:
    #Executa a transformação
    etl.landing_to_bronze()
except:
            

    #verifica se o log da execução foi salvo
    spark.sql("""
    SELECT *
    FROM monitoring.execution_logs
    WHERE pipeline_name = 'test_pipeline' AND step = 'bronze'
    ORDER BY execution_time DESC
    """).display()

    #Remover log de execução
    spark.sql("DELETE FROM monitoring.execution_logs WHERE pipeline_name = 'test_pipeline'")

    #Remover arquivo da landing
    dbutils.fs.rm(etl.path_landing, True)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Teste de função bronze to silver que deve retornar com sucesso

# COMMAND ----------

#Cria o arquivo de teste na pasta landing
log_line = '192.168.1.1 - - [10/Oct/2023:13:55:36 -0700] "GET /home HTTP/1.1" 200 1234'
dbutils.fs.put("dbfs:/landing/test_pipeline/teste.txt",log_line, True)

#Instancia o pipeline para esse teste
etl = ETLPipeline(
    spark=spark,
    pipeline_name="test_pipeline",
    file_name="teste.txt",
    source="UPLOADED",
    is_log=True,
    table_name_bronze="b_test",
    table_name_silver="s_test",
    table_name_gold="g_test",
    sql_silver="",
    sql_gold=""
)

#Executa a transformação
etl.prepare_environment()
etl.landing_to_bronze()
etl.bronze_to_silver()

#ver se criou a silver
df_silver = spark.table("silver.s_test")
display(df_silver)

#ver log de execução
spark.sql("""
        SELECT * FROM monitoring.execution_logs
        WHERE pipeline_name = 'test_pipeline' AND step = 'silver'
        ORDER BY execution_time DESC
        """).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Teste de função bronze to silver que deve retornar com erro

# COMMAND ----------

#Instancia o pipeline para esse teste
etl = ETLPipeline(
    spark=spark,
    pipeline_name="test_pipeline",
    file_name="teste.json",
    source="UPLOADED",
    is_log=True,
    table_name_bronze="b_test",
    table_name_silver="s_test",
    table_name_gold="g_test",
    sql_silver="",
    sql_gold=""
)

#Cria uma bronze manual com dado errado formato errado
dbutils.fs.put("dbfs:/bronze/test_bronze_to_silver_error/teste.json", "teste", True)

try:
    etl.bronze_to_silver()
except:
    #ver log de execução
    spark.sql("""
            SELECT * FROM monitoring.execution_logs
            WHERE pipeline_name = 'test_pipeline' AND step = 'silver'
            ORDER BY execution_time DESC
            """).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Teste de função Silver to gold que deve retornar com sucesso
# MAGIC

# COMMAND ----------

#Instancia o pipeline para esse teste
etl = ETLPipeline(
    spark=spark,
    pipeline_name="test_pipeline",
    file_name="teste.json",
    source="UPLOADED",
    is_log=True,
    table_name_bronze="b_test",
    table_name_silver="s_test",
    table_name_gold="g_test",
    sql_silver="",
    sql_gold="select * from silver.s_test"
)

#Presumindo que testamos a criação da silver acima so executar esse teste na gold que deve dar suesso
etl.silver_to_gold()



# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ####Teste de função Silver to gold que deve retornar com erro
# MAGIC

# COMMAND ----------

#Instancia o pipeline para esse teste
etl = ETLPipeline(
    spark=spark,
    pipeline_name="test_pipeline",
    file_name="teste.json",
    source="UPLOADED",
    is_log=True,
    table_name_bronze="b_test",
    table_name_silver="s_test",
    table_name_gold="g_test",
    sql_silver="",
    sql_gold=""
)

try:
    #Deve dar erro por falta de regra SQL
    etl.silver_to_gold()
except:
    #ver log de execução
    spark.sql("""
            SELECT * FROM monitoring.execution_logs
            WHERE pipeline_name = 'test_pipeline' AND step = 'gold'
            ORDER BY execution_time DESC
            """).display()
