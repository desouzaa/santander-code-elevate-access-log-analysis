# Databricks notebook source
#Imports necessários para o código rodar.
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime
import time
from zoneinfo import ZoneInfo
from pyspark.sql import Row
import requests
import json
import pandas as pd
import plotly.graph_objects as go

# COMMAND ----------

#Classe ETLPipeline contém todas as funções necessárias para a realização do ETL
class ETLPipeline:
    #O construtor recebe parâmetros que irão ser usado nas funções.
    def __init__(self, spark, pipeline_name, file_name, source, is_log,table_name_bronze, table_name_silver, table_name_gold, sql_silver = '', sql_gold = '', url = ''):
        self.spark = spark
        self.pipeline_name = pipeline_name #Nome do pipeline
        self.url = url #URL do arquivo   
        self.database_bronze = 'bronze' #Nome do database bronze, nome fixo, mas se necessário, pode ser alterado
        self.database_silver = 'silver' #Nome do database silver, nome fixo, mas se necessário, pode ser alterado
        self.database_gold = 'gold' #Nome do database gold, nome fixo, mas se necessário, pode ser alterado
        self.table_name_bronze = table_name_bronze #Nome da tabela gold
        self.table_name_silver = table_name_silver #Nome da tabela gold
        self.table_name_gold = table_name_gold #Nome da tabela gold
        self.file_name = file_name #Nome do arquivo com terminação .csv ou .txt
        self.source = source #Origem que dado será extraído
        self.sql_silver = sql_silver #SQL com regras para criação da Silver
        self.sql_gold = sql_gold #SQL com regras para criação da Gold
        self.is_log = is_log #True se arquivo é log
        self.path_landing = f"dbfs:/landing/{self.pipeline_name}/" #Path para landing
        self.path_processed = f"dbfs:/landing/{self.pipeline_name}/processed/" #Path para landing processados
        self.path_bronze = f"dbfs:/bronze/{self.pipeline_name}/{self.table_name_bronze}" #Path para bronze
        self.path_silver = f"dbfs:/silver/{self.pipeline_name}/{self.table_name_silver}" #Path para silver
        self.path_gold = f"dbfs:/gold/{self.pipeline_name}/{self.table_name_gold}" #Path para gold

    #Função que faz o safe moving
    def safe_mv(self, path_source, path_processed, file_name):
        """
        Essa função copia um arquivo entre dois paths e em seguida remove o arquivo original
        da pasta de origem de forma segura e também acrescenta no file name um timestamp.

        Parâmetros:
        path_source(str): O caminho completo do arquivo na pasta de origem.
        path_processed(str): O caminho completo da pasta de destino onde o arquivo processado será movido.

        Retorno:
        bool: Retorna True se a operação foi bem-sucedida ou False se houver algum erro durante o processo.
        """
        try:
            #Pega o datatime atual e converte num padrao bom para concatenar
            timestamp = datetime.now().strftime("%Y%m%d")
            
            #Copia o arquivop que ja foi processado para a pasta de processados. O True é recursividade
            dbutils.fs.cp(f'{path_source}{file_name}', f"{path_processed}{timestamp}/{file_name}", True)

            #Remove o arquivo antigo já processado. O True é recursividade
            dbutils.fs.rm(f'{path_source}{file_name}', True)

            #Se tudo for ok, retorna true.
            return True
        
        except Exception as e:
            #se algo der errado retorna false
            print(f"❌Erro ao mover o arquivo: {e}\n")
            return False

    #Função que verifica se path existe
    def path_exists(self, path):
        """
        Essa função verifica se o path existe, permitindo tratar melhor o código

        Parâmetros:
        path(str): O caminho completo do diretório.

        Retorno:
        bool: Retorna True se path ok, e False se não ok.
        """
        #tenta lista o path, se der ok True, se der erro False
        try:
            dbutils.fs.ls(path)
            return True
        except Exception as e:
            return False

    #Função que salva log de execução das etapas do Pipeline
    def save_execution_log(self, execution_time, pipeline_name, step, status, error_message, processed_file_path, time_elapsed):
        """
        Essa função salva um log de execução de cada parte do pipeline

        Parâmetros: 
        execution_time(str): timestamp da execução.
        pipeline_name(str): nome do pipeline que executou.
        step(str): Qual etapa do pipeline ocorreu a execução.
        status(str): Se foi bem sucedida ou mal.
        error_message(str): Mensagem de erro(se houver).
        processed_file_path(str): Path da execução.  
        time_elapsed(str): Tempo em segundos que levou de processamento.  
        """

        #Cria um dataframe com dados a serem salvos
        log_row = Row(
            execution_time=execution_time,
            pipeline_name=pipeline_name,
            step=step,
            status=status,
            error_message=error_message,
            processed_file_path=processed_file_path,
            time_elapsed = time_elapsed
        )        

        df_log = spark.createDataFrame([log_row])

        try:
            #Faz o write com mode append da tabela Delta
            df_log.write.format("delta") \
                .mode("append") \
                .saveAsTable("monitoring.execution_logs")
            print("✅Log inserido via comando DF.")  
        except:
            #Faz o write com mode append da tabela Delta
            monitoring_table_sql = f"""
            INSERT INTO monitoring.execution_logs
            VALUES (
                '{execution_time}',
                '{pipeline_name}',
                '{step}',
                '{status}',
                '{error_message.replace("'", "''")}',
                '{processed_file_path}',
                {time_elapsed}
            )
            """
            self.spark.sql(monitoring_table_sql)
            print("✅Log inserido via comando SQL.")        

    #Função que otimiza a tabela bronze gerada
    def otimization_bronze(self):
        self.spark.sql(f'''ALTER TABLE {self.database_bronze}.{self.table_name_bronze} SET TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            );''')
        print('✔️Tabela Bronze otimizada \n')

    #Função que otimiza a tabela silver gerada
    def otimization_silver(self):
        self.spark.sql(f'''ALTER TABLE {self.database_silver}.{self.table_name_silver} SET TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            );''')
        print('✔️Tabela Silver otimizada \n')

    #Função que prepara o ambiente
    def prepare_environment(self):
        """
        Essa função prepara o ambiente que será rodado o pipeline, garantindo que tudo que é necessário existe.

        Parâmetros:
        None

        Retorno:
        None

        """
        print("⚙️Iniciando preparação do ambiente...\n")
        try:
            #Criação dos databases
            print("🛢️Configurando databases...\n")
            self.spark.sql("CREATE DATABASE IF NOT EXISTS monitoring")
            self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
            self.spark.sql("CREATE DATABASE IF NOT EXISTS silver")
            self.spark.sql("CREATE DATABASE IF NOT EXISTS gold")
            self.spark.sql("CREATE DATABASE IF NOT EXISTS quality")

            #Criação do path de processed
            dbutils.fs.mkdirs(self.path_processed)

            #Criação da tabela de Monitoring
            try:
                self.spark.read.table('monitoring.execution_logs')
                print('✅Tabela execution_logs já existe!... \n')
            except:
                exist = self.path_exists('user/hive/warehouse/monitoring.db/execution_logs/')    
                if exist == True:
                    print((f'❌Tabela execution_logs não existe mas existem dados no seu path, efetuando limpeza...\n'))
                    dbutils.fs.rm('user/hive/warehouse/monitoring.db/', True)                

                self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS monitoring.execution_logs (
                    execution_time TIMESTAMP,
                    pipeline_name STRING,
                    step STRING,
                    status STRING,
                    error_message STRING,
                    processed_file_path STRING,
                    time_elapsed DOUBLE
                )
                USING DELTA
                """)               

            #Criação da tabela de Quality
            try:
                self.spark.read.table('quality.quality_checks')
                print('✅Tabela quality_checks já existe!... \n')
            except:
                exist = self.path_exists('user/hive/warehouse/quality.db/quality_checks/')    
                if exist == True:
                    print((f'❌Tabela quality_checks não existe mas existem dados no seu path, efetuando limpeza...\n'))
                    dbutils.fs.rm('user/hive/warehouse/quality.db/', True)                

                self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS quality.quality_checks (
                    pipeline_name STRING,
                    step STRING,
                    timestamp TIMESTAMP,
                    status STRING,
                    rule STRING,
                    percentage_ok DOUBLE,
                    verified_table STRING
                )
                USING DELTA
                """)
                print('✅Tabela execution_logs criada com sucesso!... \n')

            print("✅Ambiente configurado com sucesso!... \n")
        except Exception as e:
            print(f'❌Erro ao configurar o ambiente: {e}\n')    

    #Função para validar dados
    def validate_quality(self, df, step, table_name, is_log):
        """
        Essa função valida a qualidade dos dados e salva resultado na tabela 'quality.quality_checks'.
        """

        #Captura a data e hora atual
        now = datetime.now(ZoneInfo("America/Sao_Paulo"))

        #Faz count do df
        total = df.count()
        results = []


        if is_log == True:
            #Regras e campos a ser verificados
            rules = {
                "not_null_client_ip": ("client_ip", df.filter(F.col("client_ip").isNotNull()).count()),
                "not_null_endpoint": ("endpoint", df.filter(F.col("endpoint").isNotNull()).count()),
                "not_null_status_code": ("status_code", df.filter(F.col("status_code").isNotNull()).count()),
                "not_null_timestamp": ("timestamp", df.filter(F.col("timestamp").isNotNull()).count()),
                "not_zero_count": (None, total)
            }     
        
            #para cada rle_name ou seja regra, ele aplica a logica
            for rule_name, (col, valid_count) in rules.items():
                #Se for a regra not_zero_count, que tem o valor total do count
                if rule_name == "not_zero_count":
                    #verifica se o total é maior que zero e se passe True ele da como 100% a regra
                    passed = total > 0
                    percent_ok = 100.0 if passed else 0.0
                else:
                    percent_ok = (valid_count / total) * 100 if total > 0 else 0.0
                    passed = percent_ok == 100.0

                #Apenda resultados
                results.append({
                    "pipeline_name": self.pipeline_name,
                    "step": step,
                    "timestamp": now,
                    "status": "OK" if passed else "FAIL",
                    "rule": rule_name,
                    "percentage_ok": round(percent_ok, 2),
                    "verified_table": table_name
                })
        else:
            passed = total > 0
            percent_ok = 100.0 if passed else 0.0
            results.append({
                    "pipeline_name": self.pipeline_name,
                    "step": step,
                    "timestamp": now,
                    "status": "OK" if passed else "FAIL",
                    "rule": 'not_zero_count',
                    "percentage_ok": round(percent_ok, 2),
                    "verified_table": table_name
                })


        #Converte o results em DF e salva
        df_result = self.spark.createDataFrame(results)

        try:
            #Faz o write com mode append da tabela Delta
            df_result.write.format("delta") \
                .mode("append") \
                .saveAsTable("quality.quality_checks")
            print("✅Log de qualidade inserido via comando DF.\n")  
        except:

            for result in results:
                #Faz o write com mode append da tabela Delta
                quality_table_sql = f"""
                INSERT INTO quality.quality_checks
                VALUES (
                    '{result.pipeline_name}',
                    '{result.step}',
                    '{result.timestamp}',
                    '{result.status}',
                    '{result.rule}',
                    {result.percentage_ok},
                    '{result.verified_table}'
                )
                """
                self.spark.sql(monitoring_table_sql)
            print("✅Log de qualidade inserido via comando SQL.\n")  


        #Se alguma regra falhar interrompe
        if any(r["status"] == "FAIL" for r in results):
            raise ValueError(f"🔴Validação de qualidade falhou na etapa: {step}")
        else:
            print(f"✅Validação de qualidade da etapa {step} concluída com sucesso.\n")

    #Função que faz extração e ingere na landing
    def source_to_landing(self): 
        """
        Esse método executa uma extração de dados de um blob storage via HTTP público anonimamente ou move arquivo da pasta de uploads de acordo com source.

        Parâmetros: None
                    
        Retorno: None
        """       

        print('----------Source to Landing----------\n')

        #Busca a data hora atual para uso em time elapsed posteriormente
        now = datetime.now(ZoneInfo('America/Sao_Paulo'))
        start_time = time.time()
        
        #Por segurança executa a preparação do ambiente
        self.prepare_environment()
        
        try:
            print('🔵Iniciando extração de dados...\n')

            #Captura o arquivo e salva na pasta landing. Um fluxo para cada Source.

            #Se for UPLOADED, ele irá procurar o arquivo dentro do FileStore do databricks community e copiar para landing.
            if self.source == "UPLOADED":
                print('📦Acessando arquivo da pasta Uploaded...\n')
                dbutils.fs.cp(f"dbfs:/FileStore/tables/{self.file_name}", f'{self.path_landing}{self.file_name}')

            #Se for URL, irá buscar o arquivo via HTTP, e salvaar uma cópia na landing.
            elif self.source == "URL":    
                print('🌐Acessando arquivo via HTTP...\n')

                #retry de 3x para caso de indisponibilidade temporária.
                for request in range(1, 4):
                    try:
                        print(f"📥Tentativa {request}/3\n")
                        #Efetua a cópia
                        dbutils.fs.cp(self.url, f"{self.path_landing}{self.file_name}")
                        print("✅Arquivo salvo com sucesso!...\n")
                        break
                    except Exception as e:
                        print(f"⚠️Erro na tentativa {request}: {e}\n")
                        if request == 3:
                            print("❌Falha após 3 tentativas. Abortando...\n")
                            raise e 
                        else:
                            #Espera 10 segundos antes de tentar novametne.
                            time.sleep(10)           
            #calcula o time elapsed para salvar em monitoring logs
            end_time = time.time()
            time_elapsed = end_time - start_time
            self.save_execution_log(execution_time = now, pipeline_name = self.pipeline_name , step = 'landing', status = 'OK', error_message = '', processed_file_path = self.path_landing, time_elapsed = time_elapsed)
            print('✅Dados extraídos para landing com sucesso...\n')

        except Exception as e:
            print(f'❌Erro: {e}\n')
            #calcula o time elapsed para salvar em monitoring logs
            end_time = time.time()
            time_elapsed = end_time - start_time
            self.save_execution_log(execution_time = now, pipeline_name = self.pipeline_name , step = 'landing', status = 'NOTOK', error_message = f'{e}', processed_file_path = self.path_landing, time_elapsed = time_elapsed)
            raise #Emite erro para parar execução

    #Função que faz a transformação dos dados e move da landing para a bronze.
    def landing_to_bronze(self):
        """
        Esse método executa a etapa landing to bronze do ETL.

        Parâmetros: None
        
        Retorno: None
        """       
        print('----------Landing to Bronze----------\n')

        print(f'Nome tabela bronze: {self.table_name_bronze}\n')
               
        #Busca a data hora atual para ser usado em time elapsed posterioremente
        now = datetime.now(ZoneInfo('America/Sao_Paulo'))
        start_time = time.time()
        try:

            print('🔵Iniciando leitura da landing...\n')

            #verifica se path existe
            if not self.path_exists(self.path_landing):
                raise Exception(f"❌O arquivo {self.path_landing} não foi encontrado.")

            #verifica se file_name existe
            if not self.file_name:
                raise Exception(f"❌O parâmetro file_name está nulo.")

            #Verifica se é diretório válido, e se for ve se é um delta dentro
            if self.path_exists(f"{self.path_landing}{self.file_name}"):
                
                if self.file_name.lower().endswith('.txt'):
                    df_landing = self.spark.read.text(f"{self.path_landing}{self.file_name}")
                elif self.file_name.lower().endswith('.csv'):
                    df_landing = self.spark.read.csv(f"{self.path_landing}{self.file_name}")
                else:
                    #Algo está errado com o que foi passado
                    raise Exception(f"❌Tipo de arquivo não suportado: {self.file_name}")       
                

            #Cria coluna de controle, para saber data de execução
            df_bronze = df_landing.withColumn('processed_timestamp', F.lit(now))\
                                .withColumn('processed_date', F.to_date(F.lit(now)))
        
            print('🥉Salvando dados na tabela bronze...\n')

            try:
                #Tenta salvar como tabela com path (Para funcionar no databricks Community)
                df_bronze.write.format('delta') \
                .mode('overwrite') \
                .option("overwriteSchema", "true") \
                .partitionBy("processed_date") \
                .option("path", self.path_bronze) \
                .saveAsTable(f"{self.database_bronze}.{self.table_name_bronze}")

            except Exception as e:             
                ##Tenta salvar como tabela apenas (Para funcionar no databricks oficial).
                df_bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("processed_date").saveAsTable(f"{self.database_bronze}.{self.table_name_bronze}")
     
            
            print('🔄Movendo arquivo processado\n')
            #Move o arquivo já processado para uma pasta de arquivos processados
            self.safe_mv(self.path_landing, self.path_processed, self.file_name)

            #calcula o time elapsed para salvar em monitoring logs
            end_time = time.time()
            time_elapsed = end_time - start_time
            self.save_execution_log(execution_time = now, pipeline_name = self.pipeline_name , step = 'bronze', status = 'OK', error_message = '', processed_file_path = self.path_bronze, time_elapsed = time_elapsed)

            print('🥉Bronze processada com sucesso\n')
            self.otimization_bronze()

        except Exception as e:
            print(f'❌Erro: {e}\n')

            #calcula o time elapsed para salvar em monitoring logs
            end_time = time.time()
            time_elapsed = end_time - start_time
            self.save_execution_log(execution_time = now, pipeline_name = self.pipeline_name , step = 'bronze', status = 'NOTOK', error_message = f'{e}', processed_file_path = self.path_bronze, time_elapsed = time_elapsed)
            raise #Emite erro para parar execução

    #Função que faz a transformação dos dados e move da bronze para a silver.
    def bronze_to_silver(self):
        """
        Esse método executa a etapa bronze to silver do ETL.

        Parâmetros:        
        None

        Retorno: None
        """              

        print('----------Bronze to Silver----------\n')

        print(f'Nome tabela bronze: {self.table_name_bronze}\n')
        print(f'Nome tabela silver: {self.table_name_silver}\n')

        #Busca a data hora atual para calcular time elapsed 
        now = datetime.now(ZoneInfo('America/Sao_Paulo'))
        start_time = time.time()

        try:

            print('🔵Iniciando leitura da bronze...\n')

            
            #Verifica se existe o path
            if not self.path_exists(self.path_bronze): 
                #Util para quando rodar no databricks oficial
                print(f"⚠️O arquivo {self.path_bronze} não foi encontrado, então buscando tabela bronze...\n")                
                df_bronze = spark.read.table(f"{self.database_bronze}.{self.table_name_bronze}")
                   
            else:            
                #Le a tabela bronze
                df_bronze = spark.read.format('delta').load(self.path_bronze)             

            if self.is_log is True:

                if self.file_name.endswith('.txt'):
                    column_name = 'value'
                elif self.file_name.endswith('.csv'):
                    column_name = '_c0'
                else:
                    raise ValueError("Formato de arquivo não suportado. Use .txt ou .csv")
                
                #Irá aplicar o regex para extrair o valor de cada coluna baseado no tipo do arquivo
                df_silver_raw_1 = df_bronze.select(
                    F.regexp_extract(f'{column_name}', r'^(\S+)', 1).alias('client_ip'),
                    F.regexp_extract(f'{column_name}', r'\[(.*?)\]', 1).alias('timestamp'),
                    F.regexp_extract(f'{column_name}', r'\"(\S+)', 1).alias('method'),
                    F.regexp_extract(f'{column_name}', r'\"(?:\S+)\s(\S+)', 1).alias('endpoint'),
                    F.regexp_extract(f'{column_name}', r'\"(?:\S+)\s(?:\S+)\s(\S+)', 1).alias('protocol'),
                    F.regexp_extract(f'{column_name}', r'\s(\d{3})\s', 1).alias('status_code'),
                    F.regexp_extract(f'{column_name}', r'\s(\d+)$', 1).alias('response_size'),
                    F.col('processed_timestamp'),
                    F.col('processed_date'))
                
                df_silver_raw_2 = df_silver_raw_1.withColumn(
                    'response_size',
                    F.when(F.col('response_size') == '-', None).otherwise(F.col('response_size').cast(T.LongType()))
                )
                
                #Cria uma coluna com valor do endpoint limpo, ou seja sem parametros no final
                df_silver_raw_3 = df_silver_raw_2.withColumn(
                                "endpoint_clean",
                                F.when(
                                    F.col("endpoint") == "/", "/"
                                ).otherwise(
                                    F.regexp_replace(F.split(F.col("endpoint"), "\?").getItem(0), "/$", "")
                                )
                            )               
                

                
                #Cria coluna de timestamp formatado
                df_silver_raw_4 = df_silver_raw_3.withColumn('timestamp', F.to_timestamp(F.col('timestamp'), "dd/MMM/yyyy:HH:mm:ss Z"))

                #Corrige os status code nulos e response size nulos se houver
                df_silver_raw_5 = df_silver_raw_4.withColumn(
                    "status_code",
                    F.when(F.col("status_code") == "", None).otherwise(F.col("status_code"))
                )
                
                #Select final dos dados de log
                df_silver = df_silver_raw_5.select(
                    F.col('client_ip').cast(T.StringType()),
                    F.col('timestamp').cast(T.TimestampType()),
                    F.col('method').cast(T.StringType()),
                    F.col('endpoint').cast(T.StringType()),
                    F.col('endpoint_clean').cast(T.StringType()),
                    F.col('protocol').cast(T.StringType()),
                    F.col('status_code').cast(T.IntegerType()),
                    F.col('response_size').cast(T.LongType()),
                    F.col('processed_timestamp'),
                    F.col('processed_date')      
                )

            #Caso o ETL nao seja para LOG
            else:
                if self.sql_silver is None or self.sql_silver == '' or self.sql_silver == ' ':
                    raise ValueError("Parâmetro SQL está nulo ou inválido!")
                else:
                    df_silver = self.spark.sql(self.sql_silver)   

            #Cria coluna de controle, para saber data de execução
            df_silver = df_silver.withColumn('processed_timestamp', F.lit(now))\
                                .withColumn('processed_date', F.to_date(F.lit(now)))         

            print('🔎Executando Data Quality antes de salvar dados na Silver...\n')            

            self.validate_quality(df_silver, step="bronze_to_silver", table_name=self.table_name_bronze, is_log = self.is_log)

            print('🥈Salvando dados na tabela silver...\n')
            try:
                #Salva a tabela silver com formato delta, particionado por data
                df_silver.write.format('delta') \
                    .mode('overwrite') \
                    .option("overwriteSchema", "true") \
                    .partitionBy("processed_date") \
                    .option("path", self.path_silver) \
                    .saveAsTable(f"{self.database_silver}.{self.table_name_silver}")
            except Exception as e:             
                ##Tenta salvar como tabela apenas (Para funcionar no databricks oficial)
                df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("processed_date").saveAsTable(f"{self.database_silver}.{self.table_name_silver}")

            #calcula o time elapsed para salvar em monitoring logs
            end_time = time.time()
            time_elapsed = end_time - start_time
            self.save_execution_log(execution_time = now, pipeline_name = self.pipeline_name , step = 'silver', status = 'OK', error_message = '', processed_file_path = self.path_silver, time_elapsed = time_elapsed)

            print('🥈Silver processada com sucesso \n')
            self.otimization_silver()


        except Exception as e:
            print(f'❌Erro: {e}\n')

            #calcula o time elapsed para salvar em monitoring logs
            end_time = time.time()
            time_elapsed = end_time - start_time
            self.save_execution_log(execution_time = now, pipeline_name = self.pipeline_name , step = 'silver', status = 'NOTOK', error_message = f'{e}', processed_file_path = self.path_silver, time_elapsed = time_elapsed)
            raise #Emite erro para parar execução

    #Função que faz a transformação final e aplica as regras definidas em sql, para criar a tabela final
    def silver_to_gold(self):
        """
        Esse método executa a etapa silver to Gold do ETL.

        Parâmetros:
        None

        Retorno: None

        """         
        print('----------Silver to Gold----------\n')

        print(f'Nome tabela silver: {self.table_name_silver}\n')
        print(f'Nome tabela gold: {self.table_name_gold}\n')

        #Busca a data hora atual para calcular time elapsed
        now = datetime.now(ZoneInfo('America/Sao_Paulo'))
        start_time = time.time()

        try:

            print('🔵Iniciando criação da tabela gold...\n')

            #Verifica se SQL está nulo, essa etapa exige que SQL gold exista
            if self.sql_gold is None or self.sql_gold == '' or self.sql_gold == ' ':
                raise ValueError("Parâmetro SQL está nulo ou inválido!")
            else:
                df_gold = self.spark.sql(self.sql_gold)   

            #Cria coluna de controle, para saber data de execução
            df_gold = df_gold.withColumn('processed_timestamp', F.lit(now))\
                                .withColumn('processed_date', F.to_date(F.lit(now)))         

            print('🥇Salvando dados na tabela gold...\n')
            
            try:
                #Salva a tabela silver com formato delta, particionado por data
                df_gold.write.format('delta') \
                    .mode('overwrite') \
                    .option("overwriteSchema", "true") \
                    .partitionBy("processed_date") \
                    .option("path", self.path_gold) \
                    .saveAsTable(f"{self.database_gold}.{self.table_name_gold}")
            except Exception as e:             
                ##Tenta salvar como tabela apenas (Para funcionar no databricks oficial)
                df_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("processed_date").saveAsTable(f"{self.database_gold}.{self.table_name_gold}")

            #calcula o time elapsed para salvar em monitoring logs
            end_time = time.time()
            time_elapsed = end_time - start_time
            self.save_execution_log(execution_time = now, pipeline_name = self.pipeline_name , step = 'gold', status = 'OK', error_message = '', processed_file_path = self.path_gold, time_elapsed = time_elapsed)

            print('🥇Gold processada com sucesso \n')
            self.otimization_silver()


        except Exception as e:
            print(f'❌Erro: {e}\n')

            #calcula o time elapsed para salvar em monitoring logs
            end_time = time.time()
            time_elapsed = end_time - start_time
            self.save_execution_log(execution_time = now, pipeline_name = self.pipeline_name , step = 'gold', status = 'NOTOK', error_message = f'{e}', processed_file_path = self.path_gold, time_elapsed = time_elapsed)
            raise #Emite erro para parar execução

    #Função que exeucuta a orquestração das funções de ETL
    def execute_etl(self, step=4):
        """
        Executa o pipeline de ETL até a etapa/step desejada.
        
        Parâmetros:
        - step(int): até qual etapa executar:
        1 = Source to Landing
        2 = Landing to Bronze
        3 = Bronze to Silver
        4 = Silver to Gold        

        Retorno:
        None
        """

        print(f"⏩ Iniciando execução do ETL {self.pipeline_name} até o passo {step}...\n")
        

        # ✅ Validação inicial do step
        if step not in [1, 2, 3, 4]:
            raise ValueError(f"❌Step inválido: {step}. Valores aceitos são 1, 2, 3 ou 4.")


        try:
            #Executa a preparação e configuração do ambiente
            self.prepare_environment()

            if step >= 1:
                print(f'Fonte a ser extraído dado: {self.source}\n')
                self.source_to_landing()
            if step >= 2:                
                self.landing_to_bronze()
            if step >= 3:
                self.bronze_to_silver()
            if step >= 4:
                self.silver_to_gold()

            print("✅Execução do ETL finalizada com sucesso!...\n")

        except Exception as e:
            print(f"❌Erro durante a execução do ETL: {e}")
            raise

    #Função que cria gráficos com análises de logs
    def log_analysis(self):
        """
        Gerar gráficos com analises para logs
        
        Parâmetros:        
        table_gold = Nome da tabela gold de logs Apache a ser usada      

        Retorno:
        None
        """

        #Busca a tabela gold
        df = self.spark.table(f"{self.database_gold}.{self.table_name_gold}")

        #Converte em Pandas
        df_pd = df.toPandas()

        #Captura apenas a primeira linha
        row = df_pd.iloc[0]

        #Mostra dados para validação e anaálise
        display(df)

        #top ips
        df_ips = df.select('top_ips')

        df_exploded_ips = df_ips.select(F.explode("top_ips").alias("item"))

        df_top_ips = df_exploded_ips.select(df_exploded_ips.item.client_ip.alias('client_ip'), df_exploded_ips.item.access_count.alias('access_count'))

        #top endpoints
        df_endpoints = df.select('top_endpoints')

        df_exploded_endpoints = df_endpoints.select(F.explode("top_endpoints").alias("item"))

        df_top_endpoints = df_exploded_endpoints.select(df_exploded_endpoints.item.endpoint.alias('endpoint'), df_exploded_endpoints.item.access_count.alias('access_count'))

        #Gráfico 1 Top 10 Client IPs
        top_ips_df = pd.DataFrame(df_top_ips.toPandas())
        fig1 = go.Figure(data=[
            go.Bar(x=top_ips_df['client_ip'], y=top_ips_df['access_count'], marker_color='indigo')
        ])
        fig1.update_layout(title="Top 10 Client IPs por Acessos", xaxis_title="Client IP", yaxis_title="Acessos")
        fig1.show()


        #Gráfico 2 Top 6 Endpoints
        top_endpoints_df = pd.DataFrame(df_top_endpoints.toPandas())
        fig2 = go.Figure(data=[
            go.Bar(x=top_endpoints_df['endpoint'], y=top_endpoints_df['access_count'], marker_color='darkcyan')
        ])
        fig2.update_layout(title="Top 6 Endpoints mais Acessados", xaxis_title="Endpoint", yaxis_title="Acessos")
        fig2.show()

        #Gráfico 3 com dados de volume
        fig3 = go.Figure(data=[
            go.Indicator(mode="number", value=row['total_volume'], title={"text": "📦 Volume Total"}, domain={'row': 0, 'column': 0}),
            go.Indicator(mode="number", value=row['max_volume'], title={"text": "🔼 Máximo"}, domain={'row': 0, 'column': 1}),
            go.Indicator(mode="number", value=row['min_volume'], title={"text": "🔽 Mínimo"}, domain={'row': 0, 'column': 2}),
            go.Indicator(mode="number", value=row['avg_volume'], title={"text": "📉 Média"}, domain={'row': 0, 'column': 3}),
        ])

        fig3.update_layout(
            grid={'rows': 1, 'columns': 4, 'pattern': "independent"},
            title="Estatísticas de Volume de Dados"
        )

        fig3.show()

        #Gráfico 4 com dados de qtd de ips distintos e dias distintos
        fig4 = go.Figure(data=[
            go.Indicator(
                mode="number",
                value=row['distinct_ip_count'],
                title={"text": "📡 IPs distintos"},
                domain={'row': 0, 'column': 0}
            ),
            go.Indicator(
                mode="number",
                value=row['distinct_day_count'],
                title={"text": "📅 Dias distintos"},
                domain={'row': 0, 'column': 1}
            )
        ])

        fig4.update_layout(
            grid={'rows': 1, 'columns': 2, 'pattern': "independent"},
            title="Distribuição IPs e Dias"
        )

        fig4.show()


        #Gráfico 5 com dados de dia com mais client error

        dia = row['day_with_most_client_errors']  

        fig = go.Figure()

        fig.add_annotation(
            text=f"📅 <b>{dia}</b>",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=40),
            xref="paper",
            yref="paper",
            align="center"
        )

        fig.update_layout(
            title="📊 Dia com mais Client Error",
            height=300,
            margin=dict(l=20, r=20, t=50, b=20),
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            plot_bgcolor="white"
        )

        fig.show()

