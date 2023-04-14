from confluent_kafka import Producer
import paho.mqtt.client as mqtt

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("your_topic/temperature")

def on_message(client, userdata, msg):
    try:
        # Create a Kafka producer instance
        producer = Producer({'bootstrap.servers': 'localhost:9092'})

        # Send the message to the Kafka broker with the topic 'my-test'
        producer.produce('my-test', msg.payload)

        # Wait for any outstanding messages to be delivered and delivery reports received
        producer.flush()

        print("Message sent to Kafka: " + msg.payload)
    except Exception as e:
        print(e)

# Create an MQTT client instance
client = mqtt.Client()

# Set the callback functions for MQTT events
client.on_connect = on_connect
client.on_message = on_message

# Connect to the MQTT broker
client.connect("4.193.183.143", 1883, 60)

# Start the MQTT client loop
client.loop_forever()
