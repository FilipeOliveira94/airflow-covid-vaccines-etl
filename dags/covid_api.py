from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
import boto3
from decimal import Decimal
from dotenv import load_dotenv
import os
import sqlalchemy
from sklearn.preprocessing import LabelEncoder
import pyarrow.parquet as pq
import pyarrow as pa

# Reading .env variables
env = load_dotenv()
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
DB_USER = os.getenv('DB_USER')
DB_PWD = os.getenv('DB_PWD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_DATABASE = os.getenv('DB_DATABASE')
AWS_REGION = os.getenv('AWS_REGION')

def ingest_api():
    # First call args
    URL = 'https://imunizacao-es.saude.gov.br/_search?scroll=1m'
    user = 'imunizacao_public'
    pwd = 'qlto5t&7r_@+#Tlstigi'
    body = {
        "size": 1000
    }

    # First call for 10k objs and first scroll_id
    response = requests.post(URL, auth=(user, pwd), json = body)
    json_data = json.loads(response.text)
    
    # Initializing dataset with first round of data
    df = pd.json_normalize(json_data['hits']['hits'])

    # Sending following requests for the rest of the data
    all_data_list = []
    # Use next line for entire dataset, else use the range(0,2) line for testing
    #while(json_data['hits']['hits']): 
    for i in range(0,4):
        new_url = 'https://imunizacao-es.saude.gov.br/_search/scroll'
        new_body = {
            "scroll_id": json_data.get('_scroll_id'),
            "scroll": "1m"
        }
        response = requests.post(new_url, auth=(user, pwd), json = new_body)
        json_data = response.json()

        for x in range(0, len(json_data['hits']['hits'])):
            all_data_list.append(json_data['hits']['hits'][x])
    
    # Selecting the useful part of the data and appending to the initial dataset, plus removing the _source. prefix     
    df_new = pd.json_normalize(all_data_list)
    df = pd.concat([df, df_new])
    df.columns = [x.lstrip('_source').lstrip('.') for x in df_new.columns.tolist()]

    # Filtering only a subset of columns
    filter_columns = [
        'id',
        'vacina_dataAplicacao',
        'status',
        'vacina_codigo',
        'vacina_lote',
        'vacina_categoria_codigo',
        'vacina_descricao_dose',
        'vacina_grupoAtendimento_nome',
        'paciente_id',
        'paciente_nacionalidade_enumNacionalidade',
        'estabelecimento_municipio_codigo',
    ]
    df_filtered = df[filter_columns].rename(columns={'id':'vacc_id'})

    # Dropping duplicates incase of non-unique index, then duplicated rows
    df_filtered.drop_duplicates(subset='vacc_id', inplace=True)
    df_filtered.drop_duplicates(inplace=True)

    # Filling missing values with 0, for all columns
    df_filtered.fillna('0', inplace=True)

    # Converting the date columns to correct data type
    df_filtered.vacina_dataAplicacao = pd.to_datetime(df_filtered.vacina_dataAplicacao)

    # Encoding all non-encoded columns and keeping the data mappings in a variable for possible further use
    encode_cols = ['status','vacina_lote','vacina_descricao_dose','vacina_grupoAtendimento_nome','paciente_nacionalidade_enumNacionalidade']
    encoders = []
    mappings = []
    for col in encode_cols:
        encoder = LabelEncoder()
        df_filtered[col] = encoder.fit_transform(df_filtered[col])
        name_mapping = dict(zip(encoder.classes_, encoder.transform(encoder.classes_)))
        encoders.append(encoder)
        mappings.append({col:name_mapping})        
    df_filtered.rename(columns={
        'status':'status_codigo',
        'vacina_lote':'vacina_lote_codigo',
        'vacina_descricao_dose':'vacina_descricao_dose_codigo',
        'vacina_grupoAtendimento_nome':'vacina_grupo_atendimento_codigo',
        'paciente_nacionalidade_enumNacionalidade':'paciente_nacionalidade_codigo'
    }, inplace=True)

    # Converting all possible columns to int data type
    convert_cols = ['vacina_codigo','vacina_categoria_codigo','estabelecimento_municipio_codigo']
    for col in convert_cols:
        df_filtered[col] = df_filtered[col].astype('int32')
        
    # Sending the dataset to a AWS DynamoDB table
    dynamodb = boto3.resource(
        'dynamodb', 
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name = AWS_REGION
    )
    table = dynamodb.Table('covid_vaccines')
    with table.batch_writer() as batch:
        for index, row in df_filtered.iterrows():
            batch.put_item(json.loads(row.to_json(), parse_float=Decimal))
    
    # Sending the dataset to a AWS RDS table
    engine = sqlalchemy.create_engine("postgresql+psycopg2://"+DB_USER+":"+DB_PWD+"@"+DB_HOST+":"+DB_PORT+"/"+DB_DATABASE)
    df_filtered.set_index('vacc_id', inplace=True)
    df_filtered.to_sql('covid_vaccines', engine, if_exists = 'replace', index=True)
    
    # Writing parquet files partitioned by year, month and day
    df_filtered['year'] = df_filtered.vacina_dataAplicacao.dt.year
    df_filtered['month'] = df_filtered.vacina_dataAplicacao.dt.month
    df_filtered['day'] = df_filtered.vacina_dataAplicacao.dt.day
    table = pa.Table.from_pandas(df_filtered)
    pq.write_to_dataset(table, root_path='covid_vaccines',
                        partition_cols=['year', 'month', 'day'])


# DAG settings for Apache Airflow
interval = '0 7 * * *'
DAG_NAME = 'PL_ingest_api'
tag = ['covid_data']

with DAG(
    DAG_NAME,
    start_date = datetime(2023,1,17),
    schedule_interval = interval,
    tags = tag
) as dag:  
    
    Initialize = DummyOperator(
        task_id = 'Initialize',
        retries=5,
        retry_delay=timedelta(seconds=150)
    )    
    Terminate = DummyOperator(
        task_id = 'Terminate',
        trigger_rule='none_failed'
    )
    
    ingest_task = PythonOperator(
        task_id = 'ingest_task',
        python_callable = ingest_api
    )
    
    Initialize >> ingest_task >> Terminate
