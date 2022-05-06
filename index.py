from re import M
from time import sleep
import pulsar
import asyncio
import configparser
import io
import yaml

publicKeyPath = "./public.pem"
privateKeyPath = "./private.pem"
crypto_key_reader = pulsar.CryptoKeyReader(publicKeyPath, privateKeyPath)


with open("conf.yml", 'r') as f:
    valuesYaml = yaml.load(f, Loader=yaml.FullLoader)
#env
print(valuesYaml['host'])

client = pulsar.Client(valuesYaml['host'])

async def consumer_a():
    consumer = client.subscribe(topic='encryption', subscription_name='sub1', crypto_key_reader=crypto_key_reader)

    for i in range(10):
        sleep(2)
        msg = consumer.receive()
        print(str(i) + "a")
        print("Received - msg '{}' id = '{}'".format(msg.data(), msg.message_id()))
        consumer.acknowledge(message=msg)
    consumer.close()
async def producer_a():
    producer = client.create_producer(topic='encryption', encryption_key='encryption', crypto_key_reader=crypto_key_reader)
    for i in range(10):
        producer.send("encryption message '{}'".format(i).encode('utf8'))
        print("sent message '{}'".format(i))
    producer.flush()
    producer.close()

async def main():
    await consumer_a()
    await producer_a()
    print("222")
    
    client.close()

asyncio.run(main())
