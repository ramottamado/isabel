from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming import StreamingQuery

import os

JDBC_PROPERTIES: dict[str, str] = {
    "user": str(os.environ.get("JDBC_USERNAME")),
    "password": str(os.environ.get("JDBC_PASSWORD"))
}


def process_dataframe(df: DataFrame, id: int):
    write_to_postgres(df.selectExpr("name"))
    write_to_kafka(
        df.selectExpr(
            "cast(null as string) as key",
            "to_json(struct(name)) as value",
        )
    )


def write_to_postgres(df: DataFrame):
    df.write.jdbc(
        "jdbc:postgresql://postgres.nadzieja.test:5432/postgres",
        "public.test_people",
        "append",
        JDBC_PROPERTIES,
    )


def write_to_kafka(df: DataFrame):
    df.write.format("kafka").option("topic", "people").option(
        "kafka.bootstrap.servers", "kafka.nadzieja.test:9092"  # broker nodes
    ).save()


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

    writer: StreamingQuery = (
        message_df.writeStream.trigger(processingTime="10 seconds")
        .foreachBatch(process_dataframe)
        .start()
    )

    writer.awaitTermination()
