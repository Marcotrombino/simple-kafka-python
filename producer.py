import sched, time
from json import dumps
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

scheduler = sched.scheduler(time.time, time.sleep)


def produce_user(sc):
    # produce users in two different partitions based on name
    user = {
        "name": fake.name(),
        "address": fake.address()
    }

    partition = 0 if user["name"][0] < "N" else 1
    print(partition, user)
    producer.send(topic='Users', value=user, partition=partition)

    scheduler.enter(3, 1, produce_user, (sc,))


scheduler.enter(15, 1, produce_user, (scheduler,))
scheduler.run()
