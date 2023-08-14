from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming import StreamingQuery
from kafka import KafkaProducer


class ForEachWriter:
    def open(self, partition_id, epoch_id):
        # Open connection. This method is optional in Python.
        self.people_producer: KafkaProducer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            client_id="prodspark",
            acks=1,
            value_serializer=lambda m: m.encode("ascii"),
        )
        return True

    def process(self, row: Row):
        # Write row to connection. This method is NOT optional in Python.
        self.people_producer.send(topic="people", value=row["name"])
        # print(row["name"])

    def close(self, error):
        # Close the connection. This method in optional in Python.
        self.people_producer.close()
        return True


spark: SparkSession = SparkSession.builder.appName(
    "Structured Streaming Kafka Integration"
).getOrCreate()

stream_df: DataFrame = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")  # broker nodes
    .option("subscribe", "dictionary")  # topic to subscribe
    .load()
)

message_df: DataFrame = (
    stream_df.selectExpr(
        "from_json(cast(value as string), 'name STRING, company STRING') as message"
    )
    .select("message.name")
    .filter("name is not null")
)

writer = (
    message_df.writeStream.trigger(processingTime="10 seconds")
    .foreach(ForEachWriter())
    .start()
)

writer.awaitTermination()
