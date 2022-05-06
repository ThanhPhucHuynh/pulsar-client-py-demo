import pulsar

publicKeyPath = "./public.pem"
privateKeyPath = "./private.pem"
crypto_key_reader = pulsar.CryptoKeyReader(publicKeyPath, privateKeyPath)
client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe(topic='encryption', subscription_name='encryption-sub', crypto_key_reader=crypto_key_reader)
msg = consumer.receive()
print("Received msg '{}' id = '{}'".format(msg.data(), msg.message_id()))
consumer.close()
client.close()