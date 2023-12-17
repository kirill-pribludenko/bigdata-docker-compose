import os
from pyflink.common import SimpleStringSchema
from pyflink.common.typeinfo import Types, RowTypeInfo
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, \
    KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.checkpointing_mode import CheckpointingMode


def ensure_checkpoint_dir_exists(directory: str):
    if not os.path.exists(directory):
        os.makedirs(directory)
        os.chmod(directory, 0o777)


def python_data_stream_example():
    env = StreamExecutionEnvironment.get_execution_environment()
    # Set the parallelism to be one to make sure that all data
    # including fired timer and normal data are processed by the same
    # worker and the collected result would be in order which is good for
    # assertion.
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    local_checkpoint_dir = "/opt/pyflink/tmp/checkpoints/logs"
    # hdfs_checkpoint_dir = "hdfs://namenode:9870/checkpoints"
    ensure_checkpoint_dir_exists(local_checkpoint_dir)
    # Enable checkpoint every 60000 milliseconds (1 min)
    env.enable_checkpointing(60000)
    env.get_checkpoint_config() \
        .set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    # Set directory
    checkpoint_dir = f"file://{local_checkpoint_dir}"  # ,{hdfs_checkpoint_dir}"
    # Cannot save to hdfs something missing in config
    env.get_checkpoint_config().set_checkpoint_storage_dir(checkpoint_dir)

    type_info: RowTypeInfo = Types.ROW_NAMED(['device_id',
                                              'temperature',
                                              'execution_time'],
                                             [Types.LONG(),
                                              Types.DOUBLE(),
                                              Types.INT()])

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
                               .set_topic('kir-test-processed-t1')
                               .set_value_serialization_schema(SimpleStringSchema())
                               .build()
                               ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    ds = env.from_source(source,
                         WatermarkStrategy.no_watermarks(),
                         "Kafka Source")
    ds.map(TemperatureFunction(), Types.STRING()) \
        .sink_to(sink)
    env.execute_async("Devices preprocessing")


class TemperatureFunction(MapFunction):

    def map(self, value):
        device_id, temperature, execution_time = value
        return str({"device_id": device_id,
                    "temperature": temperature - 273,
                    "execution_time": execution_time})


if __name__ == '__main__':
    python_data_stream_example()
