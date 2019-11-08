from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("babylon-258211","babylon-topic")

f = open("babylon.json", "r")
for appointment in f:
	print(appointment)
	future = publisher.publish(topic_path, data=appointment.encode('utf-8'))
	print(future.result())
f.close()

