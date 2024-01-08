# Isabel

An example of Spark Structured Streaming with Kafka Integration.

## HOWTO

1. Download PostgreSQL JDBC driver from [here](https://jdbc.postgresql.org/download/postgresql-42.5.2.jar).
2. Copy the jar to directory `lib`.
3. Set `JDBC_USERNAME` and `JDBC_PASSWORD` environment variable for PostgreSQL username and password in `.env` file.
4. Run `bash ./launch.sh isabel/example.py`.

## TODO

- Add joins (static - stream & stream - stream).