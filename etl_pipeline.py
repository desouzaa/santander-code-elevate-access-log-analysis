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

# COMMAND ----------

class ETLPipeline:
    #O construtor recebe parâmetros que irão ser usado nas funções.
    def __init__(self, spark, pipeline_name, file_name, source, is_log,table_name_bronze, table_name_silver, table_name_gold, sql_silver = '', sql_gold = '', url = ''):
        self.spark = spark
        self.pipeline_name = pipeline_name
        self.url = url       
        self.database_bronze = 'bronze'
        self.database_silver = 'silver'
        self.database_gold = 'gold'
        self.table_name_bronze = table_name_bronze
        self.table_name_silver = table_name_silver
        self.table_name_gold = table_name_gold
        self.file_name = file_name
        self.source = source
        self.sql_silver = sql_silver
        self.sql_gold = sql_gold
        self.is_log = is_log
        self.path_landing = f"dbfs:/landing/{self.pipeline_name}/"
        self.path_processed = f"dbfs:/landing/{self.pipeline_name}/processed/"
        self.path_bronze = f"dbfs:/bronze/{self.pipeline_name}/{self.table_name_bronze}"
        self.path_silver = f"dbfs:/silver/{self.pipeline_name}/{self.table_name_silver}"
        self.path_gold = f"dbfs:/gold/{self.pipeline_name}/{self.table_name_gold}"

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
            print("✅ Log inserido via comando SQL.")        

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
            # cria databases e tabelas
            print("🛢️Configurando databases...\n")
            self.spark.sql("CREATE DATABASE IF NOT EXISTS monitoring")
            self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
            self.spark.sql("CREATE DATABASE IF NOT EXISTS silver")
            self.spark.sql("CREATE DATABASE IF NOT EXISTS gold")
            self.spark.sql("CREATE DATABASE IF NOT EXISTS quality")
            dbutils.fs.mkdirs(self.path_processed)

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
            print("✅Ambiente configurado com sucesso!... \n")
        except Exception as e:
            print(f'❌ Erro ao configurar o ambiente: {e}\n')    

    #Função que faz extração e ingere na landing
    def source_to_landing(self): 
        """
        Esse método executa uma extração de dados de um blob storage via HTTP público anonimamente ou move arquivo da pasta de uploads de acordo com source.

        Parâmetros: None
                    
        Retorno: None
        """       

        print('----------Source to Landing----------\n')

        #Busca a data hora atual
        now = datetime.now(ZoneInfo('America/Sao_Paulo'))
        start_time = time.time()
        
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

            end_time = time.time()
            time_elapsed = end_time - start_time
            self.save_execution_log(execution_time = now, pipeline_name = self.pipeline_name , step = 'landing', status = 'OK', error_message = '', processed_file_path = self.path_landing, time_elapsed = time_elapsed)
            print('✅Dados extraídos para landing com sucesso...\n')

        except Exception as e:
            print(f'❌Erro: {e}\n')
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
               
        #Busca a data hora atual
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
                files = dbutils.fs.ls(f"{self.path_landing}{self.file_name}")
                has_delta_log = any(f.name == '_delta_log/' for f in files)

                #Se tem arqvuio delta log:
                if has_delta_log:
                    #cria delta table
                    df_landing = self.spark.read.format("delta").load(f"{self.path_landing}{self.file_name}")
                else:
                    #Verifica para outros tipos de arquivos
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
                #Tenta salvar como tabela(Para funcionar no databricks Community)
                df_bronze.write.format('delta') \
                .mode('overwrite') \
                .option("overwriteSchema", "true") \
                .partitionBy("processed_date") \
                .option("path", self.path_bronze) \
                .saveAsTable(f"{self.database_bronze}.{self.table_name_bronze}")

            except Exception as e:             
                ##Tenta salvar como path apenas (Para funcionar no databricks oficial) e depois cria tabela
                df_bronze.write.format("delta").mode("overwrite").saveAsTable(f"{self.database_bronze}.{self.table_name_bronze}")
     
            
            print('🔄Movendo arquivo processado\n')
            #Move o arquivo já processado para uma pasta de arquivos processados
            self.safe_mv(self.path_landing, self.path_processed, self.file_name)
            end_time = time.time()
            time_elapsed = end_time - start_time
            self.save_execution_log(execution_time = now, pipeline_name = self.pipeline_name , step = 'bronze', status = 'OK', error_message = '', processed_file_path = self.path_bronze, time_elapsed = time_elapsed)

            print('🥉Bronze processada com sucesso\n')
            self.otimization_bronze()

        except Exception as e:
            print(f'❌ Erro: {e}\n')
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

        #Busca a data hora atual
        now = datetime.now(ZoneInfo('America/Sao_Paulo'))
        start_time = time.time()

        try:

            print('🔵Iniciando leitura da bronze...\n')

            
            #Verifica se existe o path
            if not self.path_exists(self.path_bronze):
                print(f"⚠️O arquivo {self.path_bronze} não foi encontrado, então buscando tabela bronze...\n")
                
                df_bronze = spark.read.table(f"{self.database_bronze}.{self.table_name_bronze}")
                   
            else:            
                #Le a tabela bronze
                df_bronze = spark.read.format('delta').load(self.path_bronze)             

            if self.is_log is True:

                df_silver_raw = df_bronze.select(
                    F.regexp_extract('value', r'^(\S+)', 1).alias('client_ip'),
                    F.regexp_extract('value', r'\[(.*?)\]', 1).alias('timestamp'),
                    F.regexp_extract('value', r'\"(\S+)', 1).alias('method'),
                    F.regexp_extract('value', r'\"(?:\S+)\s(\S+)', 1).alias('endpoint'),
                    F.regexp_extract('value', r'\"(?:\S+)\s(?:\S+)\s(\S+)', 1).alias('protocol'),
                    F.regexp_extract('value', r'\s(\d{3})\s', 1).alias('status_code'),
                    F.regexp_extract('value', r'\s(\d+)$', 1).alias('response_size'),
                    F.col('processed_timestamp'),
                    F.col('processed_date'))
                
                df_silver_raw = df_silver_raw.withColumn(
                                "endpoint_clean",
                                F.when(
                                    F.col("endpoint") == "/", "/"
                                ).otherwise(
                                    F.regexp_replace(F.split(F.col("endpoint"), "\?").getItem(0), "/$", "")
                                )
                            )
                
                df_silver_raw = df_silver_raw.withColumn('timestamp', F.to_timestamp(F.col('timestamp'), "dd/MMM/yyyy:HH:mm:ss Z"))

                
                df_silver_raw = df_silver_raw.withColumn(
                    "status_code",
                    F.when(F.col("status_code") == "", None).otherwise(F.col("status_code"))
                ).withColumn(
                    "response_size",
                    F.when(F.col("response_size") == "", None).otherwise(F.col("response_size"))
                )
                
                df_silver = df_silver_raw.select(
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

            else:
                if self.sql_silver is None or self.sql_silver == '' or self.sql_silver == ' ':
                    raise ValueError("Parâmetro SQL está nulo ou inválido!")
                else:
                    df_silver = self.spark.sql(self.sql_silver)   

            #Cria coluna de controle, para saber data de execução
            df_silver = df_silver.withColumn('processed_timestamp', F.lit(now))\
                                .withColumn('processed_date', F.to_date(F.lit(now)))         

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
                ##Tenta salvar como path apenas (Para funcionar no databricks oficial) e depois cria tabela
                df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{self.database_silver}.{self.table_name_silver}")

            end_time = time.time()
            time_elapsed = end_time - start_time
            self.save_execution_log(execution_time = now, pipeline_name = self.pipeline_name , step = 'silver', status = 'OK', error_message = '', processed_file_path = self.path_silver, time_elapsed = time_elapsed)

            print('🥈Silver processada com sucesso \n')
            self.otimization_silver()


        except Exception as e:
            print(f'❌Erro: {e}\n')
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

        #Busca a data hora atual
        now = datetime.now(ZoneInfo('America/Sao_Paulo'))
        start_time = time.time()

        try:

            print('🔵Iniciando criação da tabela gold...\n')
   
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
                ##Tenta salvar como path apenas (Para funcionar no databricks oficial) e depois cria tabela
                df_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{self.database_gold}.{self.table_name_gold}")

            end_time = time.time()
            time_elapsed = end_time - start_time
            self.save_execution_log(execution_time = now, pipeline_name = self.pipeline_name , step = 'gold', status = 'OK', error_message = '', processed_file_path = self.path_gold, time_elapsed = time_elapsed)

            print('🥇Gold processada com sucesso \n')
            self.otimization_silver()


        except Exception as e:
            print(f'❌Erro: {e}\n')
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





