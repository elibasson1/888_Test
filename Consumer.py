import json
import os
import sys

from kafka import KafkaConsumer, TopicPartition





l = []
# bootstrap_servers = "localhost:9092"
topic = 'registered_user'

# prepare consumer
tp = TopicPartition(topic, 0)
consumer = KafkaConsumer(bootstrap_servers="localhost:9092", value_deserializer=lambda m: json.loads(m.decode('utf-8')))
consumer.assign([tp])
consumer.seek_to_beginning(tp)

# obtain the last offset value
lastOffset = consumer.end_offsets([tp])[tp]
print("starting the consumer")

for message in consumer:
    l.append(message.value)
    if message.offset == lastOffset - 1:
        break
print(l)


# save result to file
with open(os.path.join(sys.path[0], "result.txt"), 'w') as filehandle:
    for consumer in l:
        filehandle.write('%s\n' % consumer)


# with open(os.path.join(sys.path[0], "resulr.txt"), "r") as f:
#     print(f.read())


# # Save result to file
# with open('result.txt', 'w') as outfile:
#     json.dump(l, outfile)
#
# # open json file
# with open('result.txt') as json_file:
#     data = json.load(json_file)
#
# # Convert list to json
# result = json.dumps(data, indent=2)
# print(result)
