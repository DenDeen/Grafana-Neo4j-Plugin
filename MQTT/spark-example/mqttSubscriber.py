import time, json
from queue import Queue
from threading import Thread
import paho.mqtt.client as mqtt
import paho.mqtt.publish as p
import finnhub

MQTT_CLIENT_NAME = "mqtt"
MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "humAIn/testTopic1"
MQTT_TOPIC2 = "humAIn/testTopic2"
COUNTER = 0

# Initiate MQTT Client
mqttClient = mqtt.Client(MQTT_CLIENT_NAME)
finnhub_client = finnhub.Client(api_key="c6jpp6qad3ic1ei3pk4g")

def accumulator():
    global COUNTER
    COUNTER += 1
    if COUNTER == 10:
        COUNTER = 0 
    return COUNTER

def mqtt_message_looper_test1():
    while True:
        time.sleep(10)

        import time
        import pandas as pd
        import json

        id = 0
        while (True):
            res = finnhub_client.general_news('general', min_id=0)
            df = pd.DataFrame(res)
            row = df.iloc[0]
            if (id != df.iloc[0].id):
                result = row.to_json()
                parsed = json.loads(result)
                id = df.iloc[0].id

            MQTT_MSG = json.dumps(json.dumps(parsed, indent=4))

            # Publish message to MQTT Broker    
            p.single(MQTT_TOPIC,MQTT_MSG, port=MQTT_PORT)
            print("Artikel met id: " + id + " verzonden.")   

def mqtt_message_looper_test2():
    while True:
        time.sleep(10)

        import time
        import pandas as pd
        import json

        id = 0
        while (True):
            res = finnhub_client.general_news('general', min_id=0)
            df = pd.DataFrame(res)
            row = df.iloc[0]
            if (id != df.iloc[0].id):
                result = row.to_json()
                parsed = json.loads(result)
                id = df.iloc[0].id

            MQTT_MSG = json.dumps(json.dumps(parsed, indent=4))

            # Publish message to MQTT Broker    
            p.single(MQTT_TOPIC,MQTT_MSG, port=MQTT_PORT)
            print("Artikel met id: " + id + " verzonden.")   

first_thread = Thread(target=mqtt_message_looper_test1)
first_thread.start()

second_thread = Thread(target=mqtt_message_looper_test2)
second_thread.start()

#   Code for listening to mqttBroker
def mqtt_client_starter():
    def on_publish(client, userdata, mid):
        print("Message has been adapted...")

    def on_connect(client, userdata, flags, rc):
        print("Connected with result code {0}".format(str(rc)))  # Print result of connection attempt
        client.subscribe(MQTT_TOPIC)  # Subscribe to the topic "humAIn/testTopic", receive any messages published on it

    def on_message(client, userdata, msg):

        payload = json.loads(msg.payload)

        print("Verrijken van het bericht")
        origineleWaarde = payload["fieldset"]["testwaarde"]
        negatief = origineleWaarde * -1
        kwadraat = origineleWaarde * origineleWaarde

        # de route maakt het makkelijk om van een mqtt bericht af te lezen hoe dit bericht werd opgesteld (welke services het heeft doorlopen)
        route = payload["route"] + "->" + "verrijkt"

        # eventuele optionaltags nemen we over
        if "optionaltags" in payload:
            optionaltags = payload["optionaltags"]

        # we bouwen onze nieuwe fieldset op
        fieldset = {
            "origineel": origineleWaarde,
            "negatief": negatief,
            "kwadraaat": kwadraat
        }

        # we maken gebruik van een functie die toelaat om snel een bericht op te bouwen (uiteraar kan je zelf ook je bericht opbouwen, zoals in het begin van dit script)
        message = {
            "measurement": "demo",
            "tagset": {
                "country": "BE",
                "site": "WLS",
                "area": "FLOOR",
                "line": "LUX18",
                "level1": "DEMO"
            },
            "optionaltags": {
                "plaatnummer": "ABC123"
            },
            "fieldset": {
                "testwaarde": COUNTER
            },
            "route": "mqttTemplate"
        }

        # we voegen zelf nog een level toe
        message["tagset"]["level1"] = "DEMO2"

        # we voegen zelf ook nog de optionaltags toe
        message["optionaltags"] = optionaltags

        MQTT_MSG = json.dumps(message)

        mqttClient.publish("humAIn/enriched", MQTT_MSG)

    # Register publish callback function
    mqttClient.on_publish = on_publish
    mqttClient.on_connect = on_connect
    mqttClient.on_message = on_message

    # Connect with MQTT Broker
    mqttClient.connect(MQTT_HOST, MQTT_PORT)
    mqttClient.loop_forever()
