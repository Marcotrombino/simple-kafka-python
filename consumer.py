from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "Users",
    group_id="group_1"
    # auto_offset_reset='earliest'
)

print("Connected...")

for msg in consumer:
    print(msg.partition, msg.value)
