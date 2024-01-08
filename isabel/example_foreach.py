from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.dataframe import DataFrame
from kafka import KafkaProducer  # type: ignore


class ForEachWriter:
    def open(self, partition_id, epoch_id) -> bool:
        # Open connection. This method is optional in Python.
        self.people_producer = KafkaProducer(
            bootstrap_servers=["kafka.nadzieja.test:9092"],
            client_id="prodspark",
            acks=1,
            value_serializer=lambda m: m.encode("ascii"),
        )

        return True

    def process(self, row: Row):
        # Write row to connection. This method is NOT optional in Python.
        self.people_producer.send(topic="people", value=row["name"])

    def close(self, error) -> bool:
        # Close the connection. This method in optional in Python.
        self.people_producer.close()

        return True


if __name__ == "__main__":
    spark: SparkSession = SparkSession.builder.appName(
        "Structured Streaming Kafka Integration"
    ).getOrCreate()

    stream_df: DataFrame = (
        spark.readStream.format("kafka")
        # broker nodes
        .option("kafka.bootstrap.servers", "kafka.nadzieja.test:9092")
        .option("subscribe", "employee")  # topic to subscribe
        .option("startingOffsets", "earliest")
        .load()
    )

    message_df: DataFrame = (
        stream_df.selectExpr(
            """from_json(
                cast(value as string), 'name STRING, company STRING'
            ) as message"""
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
