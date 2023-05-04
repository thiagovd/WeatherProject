import requests
import psycopg2
from datetime import datetime
import configparser


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get('postgrees_connection','host')
db = parser.get('postgrees_connection','database')
user = parser.get('postgrees_connection','user')
pw = parser.get('postgrees_connection','password')
port = parser.get('postgrees_connection','port')


API_key = parser.get('API_clima','API_KEY')

#### Extraindo dados da API
# link do open_weather: https://openweathermap.org/

cidade = "sao paulo"
link = f"https://api.openweathermap.org/data/2.5/weather?q={cidade}&appid={API_key}&lang=pt_br"

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
print(descricao,cidade,nascer_sol,fim_sol, f"{temperatura}ºC")
teste = [cidade,descricao,temperatura,temp_min,temp_max,nascer_sol,fim_sol,current_timestamp]
for i in teste:
    print(i, type(i))

##### Escrevendo no banco


# Connect to the database
conn = psycopg2.connect(
    host=hostname,
    database=db,
    user=user,
    password=pw,
    port=port

)

# Open a cursor to perform database operations
cur = conn.cursor()

# CRIANDO TBL WEATHER PROJECT NO BANCO POSTGREES
create_table_query = '''CREATE TABLE IF NOT EXISTS weather_project
                        (id SERIAL PRIMARY KEY,
                        cidade TEXT NOT NULL,
                        descricao TEXT,
                        temperatura TEXT,
                        temperatura_min TEXT,
                        temperatura_max TEXT,
                        nascer_sol TEXT,
                        fim_sol TEXT,
                        dt_ingestao TEXT NOT NULL);'''
cur.execute(create_table_query)
# Execute an SQL statement to insert data into a table
cur.execute("INSERT INTO weather_project (cidade,descricao,temperatura,temperatura_min,temperatura_max,nascer_sol,fim_sol,dt_ingestao) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (cidade,descricao,temperatura,temp_min,temp_max,nascer_sol,fim_sol,current_timestamp))

# Commit the transaction
conn.commit()

# Close the cursor and the connection
cur.close()
conn.close()
