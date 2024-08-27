from kafka import KafkaProducer
import json

def producer_alarm(message):
    producer=KafkaProducer(
            bootstrap_servers=['ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    data={'bot': message}
    producer.send('chat', value=data)
    producer.flush()

