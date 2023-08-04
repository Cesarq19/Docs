##############################################################################################################################
##  Tema:               Comunicacion MQTT entre RaspberryPi y ESP32                                                         ##
##  Nombre del archivo: mqtt_pub.py                                                                                         ##
##  Fecha:              27/11/2022                                                                                          ##
##  Encargado:          Milena Riquero                                                                                      ##
##  Observacion:                                                                                                            ##
##  Codigo fuente tomado de la pagina https://www.emqx.com/en/blog/how-to-use-mqtt-in-python                                ##
##############################################################################################################################

import psycopg2
import random
import time
from paho.mqtt import client as mqtt_client

broker = '192.168.0.142'
port = 1883

# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'

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
        valor_lectura = 0
        
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

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client,cursor):
    msg_count = 0
    while True:
        time.sleep(1)
        lectura(cursor)
        result_luz_sala = client.publish('luz/sala', str(dic_sensores[2]))
        # result: [0, 1]
        if result_luz_sala[0] == 0:
            print(f"Send `{dic_sensores[2]}` to topic `luz/sala`")
        else:
            print(f"Failed to send message to topic luz/sala")        
        msg_count += 1

        result_luz_frente = client.publish('luz/frente', str(dic_sensores[3]))
        # result: [0, 1]
        if result_luz_frente[0] == 0:
            print(f"Send `{dic_sensores[3]}` to topic `luz/frente`")
        else:
            print(f"Failed to send message to topic luz/frente")        
        msg_count += 1

        result_luz_frente = client.publish('luz/techo', str(dic_sensores[9]))
        # result: [0, 1]
        if result_luz_frente[0] == 0:
            print(f"Send `{dic_sensores[9]}` to topic `luz/techo`")
        else:
            print(f"Failed to send message to topic luz/stecho")        
        msg_count += 1
        
        result_rgb_color = client.publish('rgb/color', str(dic_sensores[4]))
        # result: [0, 1]
        if result_rgb_color[0] == 0:
            print(f"Send `{dic_sensores[4]}` to topic `rgb/color`")
        else:
            print(f"Failed to send message to topic rgb/color")        
        msg_count += 1
        
        result_intruso = client.publish('cuarto/intruso', str(dic_sensores[7]))
        # result: [0, 1]
        if result_intruso[0] == 0:
            print(f"Send `{dic_sensores[7]}` to topic `cuarto/intruso`")
        else:
            print(f"Failed to send message to topic cuarto/intruso")        
        msg_count += 1



def run():
    global cursor
    client = connect_mqtt()
    client.loop_start()
    lectura(cursor)
    publish(client,cursor)


if __name__ == '__main__':
    try:
        connection = connect_db()
        cursor = connection.cursor()
        sensores(cursor)
        run()
    except Exception as ex:
        print(ex)
    finally:
        connection.close()
