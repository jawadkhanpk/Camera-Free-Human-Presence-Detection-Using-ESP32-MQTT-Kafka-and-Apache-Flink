from typing import Iterable

from pyflink.common import Configuration, Time, WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.functions import FlatMapFunction, ProcessFunction
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.window import SlidingEventTimeWindows, TimeWindow


# ============================================================
# Basic Configuration
# ============================================================

BOOTSTRAP_SERVERS = "xxx"

# Input is allsensors
INPUT_TOPIC = "allsensors"

# Output is fvtt
OUTPUT_TOPIC = "fvtt"

# Your Student ID
ANALYTICS_STUDENT = "xxx"

# Student ID of the source data to be processed
SOURCE_STUDENT = "xxx"


# ============================================================
# Configuration for Presence Detection
# ============================================================

# If CO2 is greater than or equal to this value, it is judged as occupied
CO2_OCCUPIED_THRESHOLD = 700.0


# ============================================================
# Parsing Topic Names
# ============================================================

def parse_sensor_topic(topic: str):
    """
    Parses the source topic name.

    Example:
      sensors-xxx-SCD41-temperature

    Return values:
      source_student = xxx
      sensor = SCD41
      data_type = temperature
    """
    parts = topic.split("-")

    if len(parts) < 4:
        raise ValueError(f"invalid topic: {topic}")

    source_student = parts[1]
    sensor = parts[2].upper()
    data_type = "-".join(parts[3:]).replace("-", "_").lower()

    return source_student, sensor, data_type


# ============================================================
# Validation and Transformation of Input Messages
# ============================================================

class ParseAllSensorsMessage(FlatMapFunction):
    """
    Validates the String coming from allsensors and
    converts only your own sensor data into a tuple[str, int, float].

    Input example:
      sensors-xxx-SCD41-temperature,1717000000000,24.53

    Output:
      (
        "sensors-xxx-SCD41-temperature",
        1717000000000,
        24.53
      )
    """

    def flat_map(self, message: str):
        try:
            message = message.strip()
            parts = message.split(",")

            if len(parts) != 3:
                print(f"skip invalid format: {message}")
                return

            topic = parts[0].strip()
            timestamp_text = parts[1].strip()
            value_text = parts[2].strip()

            # Ignore if the analytics topic was read by mistake
            if "-analytics-" in topic:
                return

            # Verify the format of the source data topic
            if not topic.startswith("sensors-"):
                print(f"skip invalid topic: {topic}")
                return

            # Extract the student ID from the topic
            source_student, sensor, data_type = parse_sensor_topic(topic)

            # Ignore sensors other than your own
            # if source_student != SOURCE_STUDENT:
            #    return

            timestamp = int(timestamp_text)
            value = float(value_text)

            yield topic, timestamp, value

        except Exception as e:
            print(f"skip invalid message: {message}, error={e}")
            return


# ============================================================
# Timestamp Assigner for Event Time
# ============================================================

class SensorTimestampAssigner(TimestampAssigner):
    """
    Extracts the timestamp from the parsed_stream tuple.

    tuple:
      (source_topic, timestamp, value)
    """

    def extract_timestamp(self, value, record_timestamp):
        return value[1]


# ============================================================
# Generating Topic Names
# ============================================================

def make_analytics_topic(source_topic: str, stat: str):
    """
    Generates the analytics topic name for min/max/avg.

    Example:
      sensors-xxx-analytics-xxx_SCD41_avg-temperature
    """
    source_student, sensor, data_type = parse_sensor_topic(source_topic)

    return (
        f"sensors-{ANALYTICS_STUDENT}-analytics-"
        f"{source_student}_{sensor}_{stat}-{data_type}"
    )


def make_presence_topic(source_student: str, name: str, data_type: str):
    """
    Generates the topic name for presence detection.

    Example:
      sensors-xxx-analytics-xxx_PRESENCE_state-presence
      sensors-xxx-analytics-xxx_SCD41_latest-co2
    """
    return (
        f"sensors-{ANALYTICS_STUDENT}-analytics-"
        f"{source_student}_{name}-{data_type}"
    )


# ============================================================
# Window Aggregation min / max / avg
# ============================================================

class StatsWindowFunction(ProcessWindowFunction):
    """
    Calculates min / max / avg from the values of the last 5 minutes
    for each source topic.
    """

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context[TimeWindow],
        elements: Iterable[tuple],
    ):
        values = []

        for record in elements:
            source_topic, timestamp, value = record
            values.append(value)

        if len(values) == 0:
            return

        min_value = min(values)
        max_value = max(values)
        avg_value = sum(values) / len(values)

        for stat, value in [
            ("min", min_value),
            ("max", max_value),
            ("avg", avg_value),
        ]:
            analytics_topic = make_analytics_topic(key, stat)

            # Format to send to fvtt: topic,value
            yield f"{analytics_topic},{value:.2f}"


# ============================================================
# Presence Detection by CO2 Threshold
# ============================================================

class PresenceDetectionFunction(ProcessFunction):
    """
    Judges human presence using only the CO2 value of SCD41.

    Judgment rules:
      If CO2 >= CO2_OCCUPIED_THRESHOLD then Occupied
      If CO2 <  CO2_OCCUPIED_THRESHOLD then Vacant
    """

    def process_element(self, record, ctx: ProcessFunction.Context):
        source_topic, timestamp, value = record

        try:
            source_student, sensor, data_type = parse_sensor_topic(source_topic)
        except Exception:
            return

        # Do not use anything other than CO2 for presence detection
        if not (sensor == "SCD41" and data_type == "co2"):
            return

        presence = value >= CO2_OCCUPIED_THRESHOLD
        presence_value = "1" if presence else "0"

        presence_topic = make_presence_topic(
            source_student,
            "PRESENCE_state",
            "presence",
        )

        # Format to send to fvtt: topic,value
        yield f"{presence_topic},{presence_value}"

        # Output the latest CO2 value as well for confirmation
        co2_topic = make_presence_topic(
            source_student,
            "SCD41_latest",
            "co2",
        )

        yield f"{co2_topic},{value:.2f}"


# ============================================================
# Main Process
# ============================================================

def main():
    config = Configuration().set_string("python.execution-mode", "thread")

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)

    # ------------------------------------------------------------
    # 1. Read from allsensors
    # ------------------------------------------------------------
    consumer = FlinkKafkaConsumer(
        topics=INPUT_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"pyflink-{ANALYTICS_STUDENT}-analytics",
            "auto.offset.reset": "latest",
        },
    )

    raw_stream = env.add_source(consumer)

    # ------------------------------------------------------------
    # 2. Validate + Extract only your own sensor data
    # ------------------------------------------------------------
    parsed_stream = raw_stream.flat_map(
        ParseAllSensorsMessage(),
        output_type=Types.TUPLE([
            Types.STRING(),  # source_topic
            Types.LONG(),    # timestamp
            Types.DOUBLE(),  # value
        ]),
    )

    # ------------------------------------------------------------
    # 3. Handle timestamp as Event Time
    # ------------------------------------------------------------
    timed_stream = parsed_stream.assign_timestamps_and_watermarks(
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(SensorTimestampAssigner())
    )

    # ------------------------------------------------------------
    # 4. Aggregate min/max/avg every 5 minutes and 30 seconds for each source topic
    # ------------------------------------------------------------
    stats_stream = (
        timed_stream
        .key_by(lambda record: record[0])
        .window(
            SlidingEventTimeWindows.of(
                Time.minutes(5),
                Time.seconds(30),
            )
        )
        .process(
            StatsWindowFunction(),
            output_type=Types.STRING(),
        )
    )

    # ------------------------------------------------------------
    # 5. Presence Detection by CO2 Threshold
    # ------------------------------------------------------------
    presence_stream = timed_stream.process(
        PresenceDetectionFunction(),
        output_type=Types.STRING(),
    )

    # ------------------------------------------------------------
    # 6. Combine min/max/avg and presence detection results
    # ------------------------------------------------------------
    output_stream = stats_stream.union(presence_stream)

    # Debug print
    output_stream.print()

    # ------------------------------------------------------------
    # 7. Output to fvtt in topic,value format
    # ------------------------------------------------------------
    producer = FlinkKafkaProducer(
        topic=OUTPUT_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config={
            "bootstrap.servers": BOOTSTRAP_SERVERS,
        },
    )

    output_stream.add_sink(producer)

    env.execute("sensor_analytics_own_sensors_only")


if __name__ == "__main__":
    main()