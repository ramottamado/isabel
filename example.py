import time

from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.dataframe import DataFrame


JDBC_PROP = {"user": "postgres", "password": "pass"}


def process_dataframe(df: DataFrame, id):
    write_to_postgres(df.selectExpr("nama as name"))
    write_to_kafka(
        df.selectExpr(
            "cast(null as string) as key",
            "to_json(struct(nama)) as value",
        )
    )


def write_to_postgres(df: DataFrame):
    """
    docstring
    """
    df.write.jdbc(
        "jdbc:postgresql://postgres.nadzieja.test:5432/postgres",
        "public.test_people",
        "append",
        JDBC_PROP,
    )

    pass


def write_to_kafka(df: DataFrame):
    """
    docstring
    """
    df.write.format("kafka").option("topic", "people").option(
        "kafka.bootstrap.servers", "kafka.nadzieja.test:9092"  # broker nodes
    ).save()

    pass


spark = SparkSession.builder.appName(
    "Structured Streaming Kafka Integration"
).getOrCreate()

stream_df = (
    spark.readStream.format("kafka")
    .option(
        "kafka.bootstrap.servers", "kafka.nadzieja.test:9092"
    )  # broker nodes
    .option("subscribe", "dictionary")  # topic to subscribe
    .load()
)

message_df = (
    stream_df.selectExpr(
        "from_json(cast(value as string), 'nama STRING, alasan STRING') as message"
    )
    .select("message.nama")
    .filter("nama is not null")
)

writer = (
    message_df.writeStream.trigger(processingTime="10 seconds")
    .foreachBatch(process_dataframe)
    .start()
)
