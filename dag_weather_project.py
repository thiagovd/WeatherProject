import requests
import airflow
from datetime import timedelta, datetime
from airflow import DAG
import psycopg2
import configparser
#from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

# Argumentos
args = {'owner':'airflow'}

# Argumentos default
default_args ={
    'owner':'airflow',
    #'start_date':datetime,
    #'end_date':False,
    #'depends_on_past':False,
    #'email':[airflow@example.com'],
    #'email_on_failure':False,
    #'email_on_retry':False,
    'retries':1,
    'retry_delay': timedelta(minutes = 5),
}

# Cria a DAG
dag_weather_project = DAG(dag_id = 'Weather_project',
                          default_args=args,
                          #schedule_interval =' 0 0 * * *',
                          schedule_interval = '@once',
                          dagrun_timeout = timedelta(minutes = 60),
                          description = 'Job ETL de Carga no DW com Aiflow',
                          start_date = airflow.utils.dates.days_ago(1)
)

def extracao_dados():
# Extraindo dados do climma usando API


    cidade = "sao paulo"
    link = f"https://api.openweathermap.org/data/2.5/weather?q={cidade}&appid={API_KEY}&lang=pt_br"

    requisicao = requests.get(link)
    requisicao_dic = requisicao.json()
    print(requisicao_dic)
    current_timestamp =datetime.now()
    descricao = requisicao_dic['weather'][0]['description'].upper()
    temperatura = round(requisicao_dic['main']['temp'] - 273.15,2)
    temp_min = round(requisicao_dic['main']['temp_min'] - 273.15,2)
    temp_max =round(requisicao_dic['main']['temp_max'] - 273.15,2)
    cidade = requisicao_dic['name'].upper()
    nascer_sol = requisicao_dic['sys']['sunrise']
    nascer_sol = datetime.fromtimestamp(nascer_sol)
    fim_sol = requisicao_dic['sys']['sunset']
    fim_sol= datetime.fromtimestamp(fim_sol)


    return {
        'cidade': cidade,
        'descricao': descricao,
        'temperatura_atual': temperatura,
        'temperatura_minima': temp_min,
        'temperatura_maxima': temp_max,
        'nascer_do_sol': nascer_sol,
        'pôr_do_sol': fim_sol,
        'dt_ingestao': current_timestamp,
    }


sql_cria_tabela='''CREATE TABLE IF NOT EXISTS weather_project
                            (id SERIAL PRIMARY KEY,
                            cidade TEXT NOT NULL,
                            descricao TEXT,
                            temperatura TEXT,
                            temperatura_min TEXT,
                            temperatura_max TEXT,
                            nascer_sol TEXT,
                            fim_sol TEXT,
                            dt_ingestao TEXT NOT NULL);'''




def extracao_dados(**kwargs):
    # Extraindo dados do climma usando API

    cidade = "sao paulo"
    link = f"https://api.openweathermap.org/data/2.5/weather?q={cidade}&appid={API_KEY}&lang=pt_br"

    requisicao = requests.get(link)
    requisicao_dic = requisicao.json()
    print(requisicao_dic)
    current_timestamp = datetime.now()
    descricao = requisicao_dic['weather'][0]['description'].upper()
    temperatura = round(requisicao_dic['main']['temp'] - 273.15, 2)
    temp_min = round(requisicao_dic['main']['temp_min'] - 273.15, 2)
    temp_max = round(requisicao_dic['main']['temp_max'] - 273.15, 2)
    cidade =  requisicao_dic['name'].upper()
    nascer_sol = requisicao_dic['sys']['sunrise']
    nascer_sol = datetime.fromtimestamp(nascer_sol)
    fim_sol = requisicao_dic['sys']['sunset']
    fim_sol = datetime.fromtimestamp(fim_sol)

    dados = {
        'cidade':  "'"+str(cidade) + "'",
        'descricao': "'"+ str(descricao) + "'",
        'temperatura': "'" + str(temperatura)+"'",
        'temperatura_min': "'" + str(temp_min)+"'",
        'temperatura_max': "'" + str(temp_max)+ "'",
        'nascer_sol': "'"+ str(nascer_sol)+"'",
        'fim_sol': "'"+ str(fim_sol) + "'",
        'dt_ingestao':"'"+ str(current_timestamp) + "'",
    }

    sql_insere_dados = "INSERT INTO weather_project  (%s) VALUES (%s)" \
    % (','.join(dados.keys()), ','.join([item for item in dados.values()]))

    insere_dados = PostgresOperator(sql=sql_insere_dados,
                                    task_id="tarefa_insere_dados",
                                    postgres_conn_id="weather_project_airflow",
                                    params = dados,
                                    #params=extracao_dados(),
                                    dag=dag_weather_project
                                    )
    insere_dados.execute(context=kwargs)

extracao_api_task = PythonOperator(
    task_id ='extração_dados',
    python_callable = extracao_dados,
    dag = dag_weather_project,
)


# Tarefa de criação da tabela
cria_tabela = PostgresOperator(sql = sql_cria_tabela,
                               task_id = "tarefa_cria_tabela",
                               postgres_conn_id = "weather_project_airflow",
                               dag = dag_weather_project)


# Tarefa de insert na tabela


# fluxo da DAG
cria_tabela >> extracao_api_task

# Bloco main
if __name__ =='__main__':
    dag_weather_project.cli()