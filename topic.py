from kafka.admin import KafkaAdminClient, NewTopic

print("Connecting ...")

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='simpleKafkaPython'
)

print("Connected ...")

topic_list = [NewTopic(name="Users", num_partitions=2, replication_factor=1)]
admin_client.create_topics(new_topics=topic_list, validate_only=False)
