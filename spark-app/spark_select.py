from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct
import json

# Config PostgreSQL
POSTGRES_URL = "jdbc:postgresql://postgres:5432/crimenyc"
POSTGRES_PROPS = {
    "user": "crimenyc",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# Kafka config
KAFKA_SERVERS = "kafka:9092"

# Démarrer Spark
spark = SparkSession.builder.appName("DynamicSQLQueryHandler").getOrCreate()

def handle_sql_topic(spark, input_topic: str, output_topic: str):
    """
    Consomme des requêtes SQL depuis un topic Kafka, exécute chaque requête sur PostgreSQL,
    et envoie les résultats vers un topic Kafka en sortie.
    Retourne le StreamingQuery lancé.
    """
    # Lecture du topic Kafka contenant les requêtes SQL
    query_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", input_topic) \
        .option("startingOffsets", "latest") \
        .load() \
        .selectExpr("CAST(value AS STRING) as sql_query")

    # Fonction pour exécuter la requête SQL sur PostgreSQL
    def execute_query(query):
        try:
            df = spark.read.jdbc(
                url=POSTGRES_URL,
                table=f"({query}) as result",
                properties=POSTGRES_PROPS
            )
            return df
        except Exception as e:
            error = [{"error": str(e)}]
            return spark.createDataFrame(error)

    # Fonction appliquée à chaque micro-batch
    def process_batch(batch_df, batch_id):
        for row in batch_df.collect():
            query = row['sql_query']
            result_df = execute_query(query)

            # Sérialiser le résultat au format JSON
            result_json_df = result_df.select(to_json(struct(*result_df.columns)).alias("value"))

            # Écriture dans le topic Kafka de sortie
            result_json_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
                .option("topic", output_topic) \
                .save()

    # Lancer le stream et retourner l'objet StreamingQuery
    query_stream = query_df.writeStream \
        .foreachBatch(process_batch) \
        .start()

    return query_stream
