from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from io import StringIO
import pandas as pd
import requests
import json
import os


default_args = {
        'owner': 'Clarissa Souza',
        'depends_on_past': False,
        'email': ['clarissasouza950@gmail.com'],
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
      }

def dadosDiesel():
    os.makedirs ('dadosOriginais',exist_ok = True)
    url='https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vct/vendas-oleo-diesel-tipo-m3-2013-2022.csv'
    response = requests.get(url).content
    open('dadosOriginais/vendas-oleo-diesel-tipo-m3-2013-2022.csv','wb').write(response)

def dadosPetroleo():
    os.makedirs ('dadosOriginais',exist_ok = True)
    url='https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vendas-derivados-petroleo-e-etanol/vendas-derivados-petroleo-etanol-m3-1990-2022.csv'
    response = requests.get(url).content
    open('dadosOriginais/vendas-derivados-petroleo-etanol-m3-1990-2022.csv','wb').write(response)

def trataDados():
    dfDiesel=pd.read_csv('dadosOriginais/vendas-derivados-petroleo-etanol-m3-1990-2022.csv',sep=';')
    dfPetroleo=pd.read_csv('dadosOriginais/vendas-oleo-diesel-tipo-m3-2013-2022.csv',sep=';')
    dfGeral = pd.concat([dfDiesel, dfPetroleo])
    dfGeral.drop(['GRANDE REGIÃO'],axis=1,inplace=True)
    dfGeral.rename(columns={'ANO':'year_month','MÊS':'mes','UNIDADE DA FEDERAÇÃO':'uf','PRODUTO':'product','VENDAS':'volume'},inplace=True)
    dfGeral['uf'].replace(['ACRE','ALAGOAS','AMAPÁ','AMAZONAS','BAHIA','CEARÁ','DISTRITO FEDERAL','ESPÍRITO SANTO','GOIÁS','MARANHÃO','MATO GROSSO','MATO GROSSO DO SUL','MINAS GERAIS','PARANÁ','PARAÍBA','PARÁ','PERNAMBUCO','PIAUÍ','RIO DE JANEIRO','RIO GRANDE DO NORTE','RIO GRANDE DO SUL','RONDÔNIA','RORAIMA','SANTA CATARINA','SERGIPE','SÃO PAULO','TOCANTINS'],['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PR','PB','PA','PE','PI','RJ','RN','RS','RO','RR','SC','SE','SP','TO'],inplace=True)
    dfGeral['mes'].replace(['JAN','FEV','MAR','ABR','MAI','JUN','JUL','AGO','SET','OUT','NOV','DEZ'],['01','02','03','04','05','06','07','08','09','10','11','12'],inplace=True)
    dfGeral['year_month']=dfGeral['year_month'].astype(str) + '-' + dfGeral['mes']
    dfGeral.drop(['mes'],axis=1,inplace=True)
    dfGeral['unit']='m3'
    dfGeral['created_at']=pd.Timestamp.today() 
    dfGeral['volume']=dfGeral['volume'].str.replace(',','.').astype(float)
    dfGeral['year_month']= pd.to_datetime(dfGeral['year_month'] )
    dados2=dfGeral.to_json(orient = 'table')
    os.makedirs ('dadosTratados',exist_ok = True)
    out_file = open('dadosTratados/DadosDerivadosPEtroleo.json', "w")
    json.dump(dados2, out_file, indent = 6)  
    out_file.close()

with DAG ('airflow_pandas_v02',
      default_args=default_args,
      start_date = datetime(2022,11,30),
      schedule_interval='@daily',
      catchup=False ) as dag:

    inicio = EmptyOperator(
            task_id='inicio'
        )
    dadosDiesel = PythonOperator(
            task_id='dadosDiesel',
            python_callable=dadosDiesel
        )
    dadosPetroleo = PythonOperator(
            task_id='dadosPetroleo',
            python_callable=dadosPetroleo
        )
    trataDados = PythonOperator(
            task_id='trataDados',
            python_callable=trataDados
        )

    fim = EmptyOperator(
            task_id='fim'
        )


inicio >> [dadosDiesel,dadosPetroleo] >> trataDados >> fim