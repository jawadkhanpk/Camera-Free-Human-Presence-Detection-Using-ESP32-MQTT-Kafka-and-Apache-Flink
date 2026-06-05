from kafka import KafkaConsumer, KafkaProducer
from collections import deque

KAFKA_SERVER = "xxx"

KAFKA_SUB_TOPICS = [
    "sensors-xxx-BH1750-illumination",
    "sensors-xxx-SCD41-co2"
]

BH1750_TOPIC = "sensors-xxx-BH1750-illumination"
BH1750_AVG_TOPIC = "sensors-xxx-BH1750_avg-illumination"

CO2_TOPIC = "sensors-xxx-SCD41-co2"
CO2_THRESHOLD_TOPIC = "actuators-xxx-co2_threshold-crossed"

CO2_THRESHOLD = 700

consumer = KafkaConsumer(
    bootstrap_servers=[KAFKA_SERVER],
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="xxx",
    value_deserializer=lambda x: x.decode("utf-8")
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda x: str(x).encode("utf-8")
)

consumer.subscribe(KAFKA_SUB_TOPICS)

print("Subscribing to Kafka topics:")
for topic in KAFKA_SUB_TOPICS:
    print("-", topic)

print("Kafka destinations:")
print("-", BH1750_AVG_TOPIC)
print("-", CO2_THRESHOLD_TOPIC)

# Once every 15 seconds * 20 items = 300seconds which are Equivalent to the last 5 minutes
ill_queue = deque([], 20)

ill_receive_count = 0

for message in consumer:
    print("----- Received -----")
    print("Topic :", message.topic)
    print("Value :", message.value)

    # =========================
    # BH1750 Illumination Average
    # =========================
    if message.topic == BH1750_TOPIC:
        try:
            ill_value = float(message.value)
        except ValueError:
            print("Cannot convert BH1750 value to a number:", message.value)
            continue

        ill_queue.append(ill_value)
        ill_receive_count += 1

        print("BH1750 illumination:", ill_value)
        print("Queue size:", len(ill_queue))

        # Calculate and publish average value after receiving 2 times
        if ill_receive_count >= 2:
            ill_receive_count = 0

            avg_ill = sum(ill_queue) / len(ill_queue)
            avg_ill = "{:.2f}".format(avg_ill)
            avg_ill_str = str(avg_ill)
            producer.send(BH1750_AVG_TOPIC, avg_ill_str)
            producer.flush()

            print("----- BH1750 Published Average Value -----")
            print("Publish Topic:", BH1750_AVG_TOPIC)
            print("Average:", avg_ill_str)
            print("Queue size:", len(ill_queue))

    # =========================
    # SCD41 CO2 Threshold
    # =========================
    elif message.topic == CO2_TOPIC:
        try:
            co2_value = float(message.value)
        except ValueError:
            print("Cannot convert CO2 value to a number:", message.value)
            continue

        if co2_value > CO2_THRESHOLD:
            result = "yes"
        else:
            result = "no"

        producer.send(CO2_THRESHOLD_TOPIC, result)
        producer.flush()

        print("----- CO2 Published Judgment Result -----")
        print("CO2:", co2_value)
        print("Threshold:", CO2_THRESHOLD)
        print("Result:", result)
        print("Publish Topic:", CO2_THRESHOLD_TOPIC)