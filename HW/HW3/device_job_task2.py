from typing import Iterable, Tuple
from pyflink.common import Time
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.functions import ReduceFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows, \
    SlidingEventTimeWindows, EventTimeSessionWindows
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, \
    KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common.serialization import SimpleStringSchema


# Define data types
class SensorReading:
    def __init__(self, device_id, temperature, execution_time):
        self.device_id = device_id
        self.temperature = temperature
        self.execution_time = execution_time


# Custom ReduceFunction to get the max temperature
class MaxTemperatureFunction(ReduceFunction):
    def reduce(self,
               value1: SensorReading,
               value2: SensorReading) -> SensorReading:
        return value2 if value1.temperature < value2.temperature else value1


# Custom ProcessWindowFunction to get the window start time and max temperature
class MyProcessWindowFunction(ProcessWindowFunction):
    def process(self,
                key,
                context: ProcessWindowFunction.Context,
                max_readings: Iterable[SensorReading]
                ) -> Iterable[Tuple[int, SensorReading]]:
        max_reading = next(iter(max_readings))
        yield context.window.start, max_reading


def assign_timestamps_and_watermarks(element):
    return element


def python_data_stream_example():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    type_info = Types.ROW_NAMED(['device_id', 'temperature', 'execution_time'],
                                [Types.LONG(), Types.DOUBLE(), Types.LONG()])

    json_row_schema = JsonRowDeserializationSchema.builder() \
        .type_info(type_info).build()

    source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('kir-test') \
        .set_group_id('pyflink-e2e-source') \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(json_row_schema) \
        .build()

    sink = KafkaSink.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_record_serializer(KafkaRecordSerializationSchema.builder()
                               .set_topic('kir-test-processed-t2')
                               .set_value_serialization_schema(SimpleStringSchema())
                               .build()) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
        .with_timestamp_assigner(assign_timestamps_and_watermarks)

    ds = env.from_source(source, watermark_strategy, "Kafka Source")
    # ds = env.from_source(source, watermark_strategy, "Kafka Source") \
    #     .key_by(lambda value: value[0]) \
    #     .window(TumblingEventTimeWindows.of(Time.seconds(30))) \
    #     .reduce(MaxTemperatureFunction())

    # Tumbling Window
    # tumbling_windowed_stream = ds \
    #     .key_by(lambda value: value[0]) \
    #     .window(TumblingEventTimeWindows.of(Time.seconds(30))) \
    #     .reduce(MaxTemperatureFunction())

    # Sliding Window
    # sliding_windowed_stream = ds \
    #     .key_by(lambda value: value[0]) \
    #     .window(SlidingEventTimeWindows.of(Time.minutes(2),
    #                                        Time.minutes(1))) \
    #     .reduce(MaxTemperatureFunction())

    # Session Window
    session_windowed_stream = ds \
        .key_by(lambda value: value[0]) \
        .window(EventTimeSessionWindows.with_gap(Time.minutes(1))) \
        .reduce(MaxTemperatureFunction())

    # Results to Sink:
    # tumbling_windowed_stream.map(lambda value: str(value),
    #                              output_type=Types.STRING()).sink_to(sink)
    # sliding_windowed_stream.map(lambda value: str(value),
    #                             output_type=Types.STRING()).sink_to(sink)
    session_windowed_stream.map(lambda value: str(value),
                                output_type=Types.STRING()).sink_to(sink)

    env.execute("Windowed Temperature Processing")


if __name__ == '__main__':
    python_data_stream_example()
