from kafka import KafkaProducer
import time
import json


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


#  load data from file
with open('source.json') as f:
    source_data = json.load(f)

# filter date
l = []
for y in source_data:
    if y["country"] == "US":
        if y["currency"] != "USD":
            y["bet amount"] = y["bet amount"] * y["exchange_rate"]
            y.pop("currency")
            y.pop("exchange_rate")
            if (y["bet amount"]) > 100:
                y["bet type"] = "High"
            else:
                y["bet type"] = "Low"
            l.append(y)


filter_data = json.dumps(l)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=json_serializer)
print("Start Producer")
registered_user = filter_data
producer.send("registered_user", registered_user)
time.sleep(10)
