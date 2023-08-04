##############################################################################################################################
##  Tema:               Comunicacion MQTT entre RaspberryPi y ESP32                                                         ##
##  Nombre del archivo: mqtt_sub.py                                                                                         ##
##  Fecha:              27/11/2022                                                                                          ##
##  Encargado:          Milena Riquero                                                                                      ##
##  Observacion:                                                                                                            ##
##  Codigo fuente tomado de la pagina https://www.emqx.com/en/blog/how-to-use-mqtt-in-python                                ##
##############################################################################################################################

import psycopg2
import time
import random
from paho.mqtt import client as mqtt_client


broker = '192.168.0.142'
port = 1883
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'

#Diccionario de sensores y su valor sensado
dic_sensores = {}
dic_sensores = dict()

#Querys utilizados
id_sensores_query = "SELECT idsensor FROM sensor"

def connect_db():
    connection = psycopg2.connect(
        host = 'labremote-instance.ccmhvfkntrzs.us-east-1.rds.amazonaws.com',
        port = '5432',
        user = 'postgres',
        password = '14br3m0t3',
        database = 'labremote'
        )
    print("Conexion exitosa")
    return connection

def lectura(cursor):
    cursor.callproc('all_values_sensor',)
    rows = cursor.fetchall()
    for row in rows:
        valor, valor_text, id_sensor = row
        
        if id_sensor == 4:
            valor_lectura = str(valor_text)
        elif id_sensor == 5 or id_sensor == 6:
            valor_lectura = float(valor.to_eng_string(context=None))
        else:
            valor_lectura = int(valor.to_eng_string(context=None)[:1])
            
        dic_sensores[id_sensor] = valor_lectura
    print(dic_sensores)

def sensores(cursor):
    cursor.execute(id_sensores_query)
    rows = cursor.fetchall()
    for row in rows:
        id_sensor = row[0]
        dic_sensores[id_sensor] = 0

def act_value_sensor(cursor,args):
    cursor.callproc('insert_update_value_sensor',args)
        
def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def callback_temPID(client, userdata, msg):
        global connection
        global cursor
        temperatura = msg.payload.decode()
        #print(f"Received `{temperatura}` from `{msg.topic}` topic")
        temperatura = float(temperatura)
        args = (temperatura,"0,0,0",6)
        act_value_sensor = (cursor,args)
        lectura(cursor)

def callback_tempSala(client, userdata, msg):
        global connection
        global cursor
        temp_sala = msg.payload.decode()
        #print(f"Received `{temp_sala}` from `{msg.topic}` topic")
        temp_sala = float(temp_sala)
        args = (temp_sala,"0,0,0",5)
        act_value_sensor(cursor,args)
        lectura(cursor)

def subscribe(client):
    client.subscribe("pid/temperatura")
    client.subscribe("sala/temperatura")

def callback_funtions(client):
    client.message_callback_add("pid/temperatura",callback_temPID)
    client.message_callback_add("sala/temperatura",callback_tempSala)

def run():
    client = connect_mqtt()
    subscribe(client)
    callback_funtions(client)
    client.loop_forever()

if __name__ == '__main__':    
    try:
        connection = connect_db()
        connection.autocommit = True
        cursor = connection.cursor()
        sensores(cursor)
        run()
    except Exception as ex:
        print(ex)
    finally:
        connection.close()
    