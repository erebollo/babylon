from google.cloud import pubsub_v1
import json    
import io
import avro.schema
import avro.io

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("babylon-258211","babylon-topic")

schema_str = open("appointments.avsc", "rb").read().decode("utf-8")
schema = avro.schema.Parse(schema_str)
writer = avro.io.DatumWriter(schema)

f = open("babylon.json", "r")
for appointment in f:
	print(appointment)
	app_json = json.loads(appointment)
	bytes_writer = io.BytesIO()
	encoder = avro.io.BinaryEncoder(bytes_writer)
	writer.write(app_json, encoder)	
	future = publisher.publish(topic_path, data=bytes_writer.getvalue())
	print(future.result())
f.close()





