# ♨️ Web Server Log Analysis - Code Elevate Santander ♨️

## Descrição

Este projeto simula um fluxo de ETL para tratar dados de logs de servidores web, estruturando-os em camadas para posterior geração de métricas analíticas.

As ferramentas escolhidas para executar a tarefa foram o **Databricks Community Edition** em conjunto com o **Delta Lake** para armazenamento dos dados, pelos seguintes motivos:

- **Databricks** é uma plataforma focada em análise de dados de **Big Data**, com processamento distribuído em clusters e **Spark nativo**, o que atende diretamente aos requisitos da tarefa, sendo também uma das melhores opções para esse tipo de processamento.
- Em comparação com o uso do **Docker**, o Databricks requer muito menos esforço de configuração, pois oferece clusters de fácil criação sem a necessidade de instalação manual do Spark ou de gerenciamento de dependências.
- O **Delta Lake** é suportado nativamente pelo Databricks, o que proporciona vantagens como transações ACID, versionamento de dados e maior eficiência nas consultas. Isso torna a combinação **Databricks + Delta Lake** mais adequada do que utilizar bancos de dados relacionais ou NoSQL para este cenário.

Como o ambiente utilizado é o **Databricks Community Edition** (versão gratuita, não voltada para produção), algumas adaptações no código foram necessárias para simular um ambiente produtivo.  
Entre as adaptações, destacam-se:

- Utilização do **FileStore** nativo do Databricks para armazenamento dos dados, em vez de um Data Lake externo como S3 ou Azure Data Lake.
- Criação de um **Storage Account** na **Azure**, contendo um **Blob Storage** onde o arquivo de log foi disponibilizado publicamente, permitindo que o pipeline consuma o log diretamente via URL, além da opção de realizar upload manual para o Databricks.

Essas escolhas visaram garantir praticidade, atender todos os requisitos do desafio e manter boas práticas de engenharia de dados mesmo em ambiente de simulação.

---

## Flexibilidade e Generalização do Projeto

O projeto, apesar de ser direcionado para solucionar o desafio proposto, vai além, pois permite que o código de ETL extraia e trate outros tipos de dados além do log fornecido, uma vez que foi desenvolvido de forma totalmente **parametrizável**.

A estrutura do código foi construída seguindo princípios de:

- **Orientação a Objetos**;
- **Modularidade**;
- **Reutilização de Código**;
- **Separação de Responsabilidades**.

Isso facilita:

- Adaptar para diferentes fontes de dados (não apenas o log fornecido);
- Parametrizar o nome da execução, o nome das tabelas geradas e a origem dos dados;
- Realizar extrações tanto de arquivos via HTTP quanto de APIs públicas.

Como mencionado anteriormente, por se tratar de um projeto público e desenvolvido em ambiente limitado (**Databricks Community Edition**), foi mantida a simplicidade no acesso às APIs públicas, focando apenas em **requisições do tipo GET sem autenticação**.  
Também foi considerado o uso de arquivos via upload para o **FileStore** do Databricks, simulando uma estrutura de Data Lake simplificada.

---

## Benefícios do Projeto

- **Facilidade de manutenção**: alterações em uma parte do pipeline não impactam o todo;
- **Escalabilidade**: novos tipos de dados ou novas fontes podem ser adicionados com poucas modificações;
- **Padronização**: todos os fluxos seguem a mesma estrutura de camadas (Landing → Bronze → Silver → Gold);
- **Reaproveitamento**: funções e classes podem ser reutilizadas em outros projetos de Data Engineering;
- **Facilidade de testes**: modularidade permite testar componentes de forma isolada;
- **Organização**: separação clara entre responsabilidades de extração, transformação e carga.

---

Assim, mesmo com limitações impostas pelo ambiente, o projeto apresenta uma arquitetura sólida e boas práticas de Engenharia de Dados aplicadas a um cenário realista.

---

## Arquitetura da Solução

A arquitetura implementada segue o conceito de camadas de dados:

- **Landing**: Dados brutos recebidos no formato de origem.
- **Bronze**: Ingestão dos dados crus em formato estruturado.
- **Silver**: Transformação dos dados para formato tabular estruturado, efetuando limpezas, conversões, etc.
- **Gold**: Agregações e análises avançadas, gerando tabelas de métricas e insights.

Além disso, as tabelas foram organizadas para permitir consultas analíticas eficientes.

---

## Tecnologias Utilizadas

- **Apache Spark 3.3+**
- **Python 3.9+**
- **Databricks Community Edition**
- **Delta Lake**
- **SQL(Spark SQL)**

---

## Estrutura de Pastas

```plaintext
/
├── etl_pipeline.py           #Funções principais do pipeline ETL
├── Run.py                    #Script de execução parametrizada
├── README.md                  #Documentação do projeto
