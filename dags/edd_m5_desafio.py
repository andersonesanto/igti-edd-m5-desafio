from airflow.decorators import dag, task
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import boto3
import certifi
import json
import os
import pandas as pd
import pymongo as pm
import requests
import numpy

# variaveis de configuracao
base_path = "/usr/local/airflow/data"
# conexao mongodb Airflow_Variables
db_user = Variable.get('db_user')
db_pass = Variable.get('db_password')
db_url = "unicluster.ixhvw.mongodb.net"
db_name = "ibge"
db_collection_name = "pnadc20203"

export_file_name = "pnadc20203.json"

# AWS credenciais salvas no Airflow_Variables
aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')
aws_bucket_name = "igti-edd-m5-desafio-597495568095"

# Rest service ibge
ibge_regioes_rest_url = 'https://servicodados.ibge.gov.br/api/v1/localidades/estados'

# conexao com postgres Airflow_Variables
pg_user = Variable.get('pg_user')
pg_password = Variable.get('pg_password')
pg_hostname = Variable.get('pg_hostname')
pg_port = Variable.get('pg_port')
pg_dbname = Variable.get('pg_dbname')
pgconstring = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_hostname}:{pg_port}/{pg_dbname}"

default_args = {
    'owner': "Anderson Santo",
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 17, 19)
}

#########################################
@dag(default_args=default_args, schedule_interval=None,  description="ETL IGTI edd modulo 5 desafio final ", tags=["Postgres", "Taskflow"])
def edd_m5_desafio():

    @task
    def start():
        print("Start!")
        return True

    @task
    def end():
        print("End of Flow")
        return True

    # @task
    # def df_to_json(workdf):
    #     # exporta os dados do dataframe para json
    #     os.makedirs(db_name, exist_ok=True)
    #     dfraw.to_json(f"{db_collection_name}/{export_file_name}")

    #     return True

    @task
    def upload_json_s3():
        # upload do arquivo gerado para o S3

        s3session = boto3.Session(aws_access_key_id=aws_access_key_id,
                                  aws_secret_access_key=aws_secret_access_key)
        s3 = s3session.resource('s3')
        s3.meta.client.upload_file(f"{base_path}/{export_file_name}",
                                   aws_bucket_name,
                                   export_file_name)

        return True

    @task
    def mongodb_to_local_json():
        # configura conexao mongodb
        connection_string = f"mongodb+srv://{db_user}:{db_pass}@{db_url}/{db_name}?retryWrites=true&w=majority"
        client = pm.MongoClient(connection_string, tlsCAFile=certifi.where())
        db = client[db_name]

        # define o cursor com os filtros de dados e prepara o cursor par leitura
        mycursor = db.pnadc20203.find({
            "sexo": "Mulher",
            "$and": [
                {"idade": {"$gte": 20}},
                {"idade": {"$lte": 40}}
            ]
        })

        # carrega o conteudo do cursor em um dataframe
        # data = list(mycursor)
        # os.makedirs(db_name, exist_ok=True)
        # with open(f"{base_path}/{export_file_name}","w") as writer:
        #     writer.write(str(data))

        df = pd.DataFrame(mycursor)
        df.to_json(f"{base_path}/{export_file_name}",default_handler=str)
        return True

    # @task
    # def get_ibge_regioes(retorno):
    #     # obter dataframe a partir da tabela de regioes do IBGE
    #     r = requests.get(ibge_regioes_rest_url)
    #     rest_json = r.json()
    #     dfregioes = pd.json_normalize(rest_json)

    #     return dfregioes

    @task
    def process_and_load():

        # obter dftradado do json
        # dftratado = dfraw.copy()
        dftratado = pd.read_json(f"{base_path}/{export_file_name}")
        dftratado['id'] = dftratado['_id'].astype('|S').str.decode('utf-8')
        dftratado.drop(['_id'], axis='columns', inplace=True)

        r = requests.get(ibge_regioes_rest_url)
        rest_json = r.json()
        dfregioes = pd.json_normalize(rest_json)

        dfmerged = dftratado.merge(dfregioes
                                    ,left_on='uf'
                                    ,right_on='nome'
                                    ,how='outer'
                                   )
        # abre conexao com o postgres
        engine = create_engine(pgconstring,
                               pool_recycle=3600)
        postgreSQLConnection = engine.connect()

        # insere o conteudo do dataframe dftratado no postgres
        dfmerged.to_sql(db_collection_name,
                        postgreSQLConnection,
                        index=False,
                        if_exists='replace',
                        method='multi')
        return True

    # @task
    # def load_data_into_postgres(dfmerged):
    #     # abre conexao com o postgres
    #     engine = create_engine(pgconstring,
    #                            pool_recycle=3600)
    #     postgreSQLConnection = engine.connect()

    #     # insere o conteudo do dataframe dftratado no postgres
    #     dfmerged.to_sql(db_collection_name,
    #                     postgreSQLConnection,
    #                     index=False,
    #                     if_exists='append')

    #    return True

    ##############################################################

    start() >> mongodb_to_local_json() >> [ upload_json_s3(), process_and_load() ] >> end()

    
exec = edd_m5_desafio()
