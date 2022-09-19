from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BaschPythonOperator
from airflow.operators.bash import BaschOperator
import pandas as pd


def ler_arquivo_comb():
      path='c:/airflow-docker/vendas-combustiveis-m31.ods'
      df_comb=pd.read_excel(path,sheet_name='DPCache_m3')      
      return df_comb

def trata_colunas_comb(df):
      df_comb=df.xcom_pull(task_ids=ler_arquivo_comb)
      df_comb.drop(['REGIÃO'],axis=1,inplace=True)
      df_comb.drop(['TOTAL'],axis=1,inplace=True)
      df_comb['ESTADO'].replace(['ACRE','ALAGOAS','AMAPÁ','AMAZONAS','BAHIA','CEARÁ','DISTRITO FEDERAL','ESPÍRITO SANTO','GOIÁS','MARANHÃO','MATO GROSSO','MATO GROSSO DO SUL','MINAS GERAIS','PARANÁ','PARAÍBA','PARÁ','PERNAMBUCO','PIAUÍ','RIO DE JANEIRO','RIO GRANDE DO NORTE','RIO GRANDE DO SUL','RONDÔNIA','RORAIMA','SANTA CATARINA','SERGIPE','SÃO PAULO','TOCANTINS'],['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PR','PB','PA','PE','PI','RJ','RN','RS','RO','RR','SC','SE','SP','TO'],inplace=True)
      df_comb.rename(columns={'ESTADO':'UF'},inplace=True)
      return df_comb

def transforma_comb(df):
      df_c=df.xcom_pull(task_ids=trata_colunas_comb)
      lista_mes_1=['01','02','03','04','05','06','07','08','09','10','11','12']
      relacao_colunas = {'COMBUSTÍVEL':'COMBUSTÍVEL', 'UF':'UF', 'ANO':'ANO','VOLUME':'VOLUME'}
      lista_ano=[2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020]
      for i in range (0,21):
            filtroano=df_c['ANO'] == lista_ano[i]
            df_temp=df_c.loc[filtroano]
            df_temp1=df_temp[['COMBUSTÍVEL','UF','ANO']]
            for j in range (0,12):
                  df_temp1['VOLUME']=df_temp[df_temp.columns[j+3]]
                  df_temp1['ANO']=str(lista_ano[i])+lista_mes_1[j]
                  if j==0:
                        df_geral_t=df_temp1.copy()
                  else:
                        df_geral_t = pd.concat([df_geral_t, df_temp1.rename(columns=relacao_colunas)])
            if i==0:
                  df_geral=df_geral_t.copy()
            else:
                  df_geral = pd.concat([df_geral, df_geral_t.rename(columns=relacao_colunas)])
      return df_geral

def ajusta_colunas_c(df):
      df_ajust_col=df.xcom_pull(task_ids=transforma_comb)
      df_ajust_col['ANO'] = pd.to_datetime(df_ajust_col['ANO'], format='%Y%m', errors='coerce')
      df_ajust_col['ANO'] = df_ajust_col['ANO'].dt.strftime('%Y-%m-%d')
      df_ajust_col.rename(columns={'COMBUSTÍVEL':'product'},inplace=True)
      df_ajust_col.rename(columns={'ANO':'year_month'},inplace=True)
      df_ajust_col.rename(columns={'UF':'uf'},inplace=True)
      df_ajust_col.rename(columns={'VOLUME':'volume'},inplace=True)
      df_ajust_col['unit']='(m3)'
      df_ajust_col['product'].replace(['ETANOL HIDRATADO (m3)','GASOLINA C (m3)','GASOLINA DE AVIAÇÃO (m3)','GLP (m3)','QUEROSENE DE AVIAÇÃO (m3)','QUEROSENE ILUMINANTE (m3)','ÓLEO COMBUSTÍVEL (m3)','ÓLEO DIESEL (m3)'],['ETANOL HIDRATADO','GASOLINA C','GASOLINA DE AVIACAO','GLP','QUEROSENE DE AVIACAO','QUEROSENE ILUMINANTE','OLEO COMBUSTIVEL','OLEO DIESEL'],inplace=True)
      df_ajust_col['created_at']=datetime.today().strftime('%Y-%m-%d')
      return df_ajust_col

def ler_arquivo_oleo():
      path='c:/airflow-docker/vendas-combustiveis-m31.ods'
      df_oleo=pd.read_excel(path,sheet_name='DPCache_m3_2')    
      return df_oleo

def trata_colunas_oleo(df):
      df_oleo=df.xcom_pull(task_ids=ler_arquivo_oleo)
      df_oleo.drop(['REGIÃO'],axis=1,inplace=True)
      df_oleo.drop(['TOTAL'],axis=1,inplace=True)
      df_oleo['ESTADO'].replace(['ACRE','ALAGOAS','AMAPÁ','AMAZONAS','BAHIA','CEARÁ','DISTRITO FEDERAL','ESPÍRITO SANTO','GOIÁS','MARANHÃO','MATO GROSSO','MATO GROSSO DO SUL','MINAS GERAIS','PARANÁ','PARAÍBA','PARÁ','PERNAMBUCO','PIAUÍ','RIO DE JANEIRO','RIO GRANDE DO NORTE','RIO GRANDE DO SUL','RONDÔNIA','RORAIMA','SANTA CATARINA','SERGIPE','SÃO PAULO','TOCANTINS'],['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PR','PB','PA','PE','PI','RJ','RN','RS','RO','RR','SC','SE','SP','TO'],inplace=True)
      df_oleo.rename(columns={'ESTADO':'UF'},inplace=True)
      return df_oleo

def transforma_oleo(df):
      df_o=df.xcom_pull(task_ids=trata_colunas_oleo)
      lista_mes_1=['01','02','03','04','05','06','07','08','09','10','11','12']
      relacao_colunas = {'COMBUSTÍVEL':'COMBUSTÍVEL', 'UF':'UF', 'ANO':'ANO','VOLUME':'VOLUME'}
      lista_ano=[2013,2014,2015,2016,2017,2018,2019,2020]
      for i in range (0,8):
            filtroano=df_o['ANO'] == lista_ano[i]
            df_temp=df_o.loc[filtroano]
            df_temp1=df_temp[['COMBUSTÍVEL','UF','ANO']]
            for j in range (0,12):
                  df_temp1['VOLUME']=df_temp[df_temp.columns[j+3]]
                  df_temp1['ANO']=str(lista_ano[i])+lista_mes_1[j]
                  if j==0:
                        df_geral_t=df_temp1.copy()
                  else:
                        df_geral_t = pd.concat([df_geral_t, df_temp1.rename(columns=relacao_colunas)])
            if i==0:
                  df_geral=df_geral_t.copy()
            else:
                  df_geral = pd.concat([df_geral, df_geral_t.rename(columns=relacao_colunas)])
      return df_geral

def ajusta_colunas_o(df):
      df_ajust_col=df.xcom_pull(task_ids=ler_arquivo_oleo)
      df_ajust_col['ANO'] = pd.to_datetime(df_ajust_col['ANO'], format='%Y%m', errors='coerce')
      df_ajust_col['ANO'] = df_ajust_col['ANO'].dt.strftime('%Y-%m-%d')
      df_ajust_col.rename(columns={'COMBUSTÍVEL':'product'},inplace=True)
      df_ajust_col.rename(columns={'ANO':'year_month'},inplace=True)
      df_ajust_col.rename(columns={'UF':'uf'},inplace=True)
      df_ajust_col.rename(columns={'VOLUME':'volume'},inplace=True)
      df_ajust_col['unit']='(m3)'
      df_ajust_col['product'].replace(['ÓLEO DIESEL (OUTROS ) (m3)','ÓLEO DIESEL MARÍTIMO (m3)','ÓLEO DIESEL S-10 (m3)','ÓLEO DIESEL S-1800 (m3)','ÓLEO DIESEL S-500 (m3)'],['OLEO DIESEL (OUTROS )','OLEO DIESEL MARITIMO','OLEO DIESEL S-10','OLEO DIESEL S-1800','OLEO DIESEL S-500'],inplace=True)
      df_ajust_col['created_at']=datetime.today().strftime('%Y-%m-%d')
      return df_ajust_col

def junta_dados(df_c,df_o):
      df_geral_comb=df_c.xcom_pull(task_ids=ajusta_colunas_c)
      df_geral_oleo=df_o.xcom_pull(task_ids=ajusta_colunas_o)
      relacao_colunas = {'product':'product','uf':'uf','year_month':'year_month','volume':'volume','unit':'unit','created_at':'created_at'}
      df_junto = pd.concat([df_geral_comb, df_geral_oleo.rename(columns=relacao_colunas)])
      return df_junto

def gerar_json(df_desafio):
      df_desafio=df_desafio.xcom_pull(task_ids=junta_dados)
      df_desafio.to_json('desafio_raizen.json',orient = 'records')


with DAG('dag-raizen', start_date = datetime(2022,9,1),schedule_interval='30 * * * *', catchup=False) as dag:

      ler_arquivo_comb = PythonOperator(
            task_id='ler_arquivo_comb',
            python_callable = ler_arquivo_comb
          )

      trata_colunas_comb = PythonOperator(
            task_id='trata_colunas_comb',
            python_callable = trata_colunas_comb
          )
      
      transforma_comb = PythonOperator(
            task_id='transforma_comb',
            python_callable = transforma_comb
          ) 
      
      ajusta_colunas_c = PythonOperator(
            task_id='ajusta_colunas_c',
            python_callable = ajusta_colunas_c
          ) 

      ler_arquivo_oleo = PythonOperator(
            task_id='ler_arquivo_oleo',
            python_callable = ler_arquivo_oleo
          )

      trata_colunas_oleo = PythonOperator(
            task_id='trata_colunas_oleo',
            python_callable = trata_colunas_oleo
          )
      
      transforma_oleo = PythonOperator(
            task_id='transforma_oleo',
            python_callable = transforma_oleo
          ) 
      
      ajusta_colunas_o = PythonOperator(
            task_id='ajusta_colunas_o',
            python_callable = ajusta_colunas_o
          ) 

      junta_dados = PythonOperator(
            task_id='junta_dados',
            python_callable = junta_dados
          )

      gerar_json = PythonOperator(
            task_id='gerar_json',
            python_callable = gerar_json
          )

ler_arquivo_comb >> trata_colunas_comb >> transforma_comb >> ajusta_colunas_c >> ler_arquivo_oleo >> trata_colunas_oleo >> transforma_oleo >> ajusta_colunas_o >> junta_dados >> gerar_json

