# MBA em Engenharia de Dados
## Bootcamp - Engenheiro de dados
### Módulo 5 - Desafio Final
### Professor: Neylson Crepalde (http://github.com/neysoncrepalde)
#### Aluno: Anderson E Santo
***
## Desafio final do BootCamp
### Objetivos
Exercitar os seguintes conceitos trabalhados no Curso:
- Pipelines de Dados.
- Containers.
- Bases de Dados SQL e NoSQL.
- Conexão a APIs.
- ETL.
- Data Lake.
- Processamento de dados distribuído
### Enunciado
Você foi contratado pela empresa (fictícia) #VamosJuntos - Desenvolvimento Social para desenvolver o seu primeiro projeto de Dados. Em seu trabalho investigativo preliminar, você já identificou que as principais fontes de dados necessárias são:  
- Uma Database MongoDB disponível na nuvem para consulta.
- Uma API do IBGE (https://servicodados.ibge.gov.br/api/docs/localidades) para extração de informação de regiões, mesorregiões e microrregiões no Brasil.

Você deve, portanto, construir um pipeline de dados que faça a extração dos dados no MongoDB e na API do IBGE e deposite no Data Lake da empresa. Após a ingestão dos dados no Data Lake, você deve disponibilizar o dado tratado e filtrado apenas para o público de interesse da empresa em um DW. Com os dados no DW, você vai realizar algumas consultas e extrair resultados importantes para a #VamosJuntos.
### Atividades
Você deverá desempenhar as seguintes atividades:
1. Subir o Airflow localmente em uma estrutura de containers, usando docker-compose para utilização mais robusta (https://github.com/neylsoncrepalde/docker-airflow);  
2. Criar uma conta free tier na AWS para realização das atividades;
3. Criar um bucket no serviço S3 com o nome igti_bootcamp_ed_2021_<numero_da_sua_conta>;
4. Criar uma instância RDS de banco de dados relacional de sua escolha (pode criar a instância de DEV de 1CPU e 1GB de RAM, pois ela faz parte do free tier);
5. Construir um pipeline que faz a captura de dados do MongoDB e da API do IBGE e deposita no S3;
6. O pipeline também deve fazer a ingestão na base de dados SQL que estará servindo como DW;
7. Para persistir os dados no DW, você deve ingerir apenas os dados referentes ao público alvo da #VamosJuntos, a saber, mulheres de 20 a 40 anos;
8. Conectar seu cliente favorito no DW e realizar consultas para responder às perguntas do desafio.
### Execução
- Criação do projeto no github;  
- Criação da instância postgres no serviço AWS RDS;
- Criação do Bucket especificado no serviço AWS S3;
- Desenvolvimento do código Python no notebook chamado rascunho.ipynb;  
- Configuração e execução do AirFlow usando o docker-compose fornecido no projeto https://github.com/neylsoncrepalde/docker-airflow ;
- Implementação do código desenvolvido em um script python no padrão DAG usado pelo AirFlow;
- Testes;
- Adaptação do código ao funcionamento do AirFlow;

### Variáveis que devem ser criadas no AirFlow
| variavel | descrição |
| -- | -- |
| db_user | usuário de conexão no mongodb
| db_password | senha do usuário do mongodb
| aws_access_key_id |
| aws_secret_access_key 
| pg_user | usuário do banco postgres
| pg_password | senha do usuário do banco postgres
| pg_hostname | nome do servidor (dns ou ip) do banco postgres
| pg_port | porta para conexao  do banco postgres
| pg_dbname | nome do database do banco postgres
### Problemas enfrentados
O código escrito no notebook jupyter contava com a reutilização dos DataFrames entre um passo e outro, sem a necessidade de criação de arquivos indermediários. Ao tentar usar este conceito no AirFlow, observei que a passagem de parâmetros entre as @Tasks do Airflow só permite dados simples. Desta forma, o código foi adaptado para o seguinte fluxo:  

![fluxo](https://github.com/andersonesanto/igti-edd-m5-desafio/raw/main/assets/fluxo-tasks.png?raw=true)  

- @start
    - marca de início, sem atividade
- @mongodb_to_local_json 
    - conecta com mongodb, 
    - cria o filtro conforme o critério "Mulheres entre 20 e 40 anos"
    - cria um dataframe contendo os dados retornados pelo cursor do mongodb;
    - cria o arquivo json local;
- @upload_json_s3
    - envia o arquivo json criado para o bucket do S3
- @process_and_load
    - carrega o arquivo json local para um novo dataframe;
    - trata a coluna _id do json carregado, transformando para string;
    - carrega o json contendo as regiões a partir de uma chamada na api do IBGE;
    - normaliza os dados carregados no dataframe das regiões;
    - efetua o merge dos dataframes pnadc e regiões;
    - envia para o banco postgres;
- @end
    - marca de final de processamento, sem atividade;

### TO-DO
- Reorganizar os diretórios locais de staging;
- Terraform para criação do bucket no AWS/S3;
- Terraform para criação do banco de dados AWS/RDS/Postgres;

### Referências
- Vídeo Aulas - IGTI - BootCamp Engenheiro de dados, módulo 4, Pipelines de dados (Prof. Dr. Neylson Crepalde)
- https://github.com/neylsoncrepalde/docker-airflow  
- https://pythontic.com/pandas/serialization/postgresql  
- https://medium.com/@apoor/quickly-load-csvs-into-postgresql-using-python-and-pandas-9101c274a92f  
- https://chartio.com/resources/tutorials/how-to-execute-raw-sql-in-sqlalchemy/  