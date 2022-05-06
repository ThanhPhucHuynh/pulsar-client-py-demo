import pulsar

publicKeyPath = "./public.pem"
privateKeyPath = "./private.pem"
crypto_key_reader = pulsar.CryptoKeyReader(publicKeyPath, privateKeyPath)
client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer(topic='encryption', encryption_key='encryption', crypto_key_reader=crypto_key_reader)
producer.send('encryption message'.encode('utf8'))
print('sent message')
producer.close()
client.close()