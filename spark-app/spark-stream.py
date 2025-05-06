#!/usr/bin/env python3
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, lit, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from functools import reduce
import json
import logging
from pyspark.sql import Row

# Configuration du logger pour afficher uniquement les erreurs
logging.basicConfig(level=logging.ERROR, format='%(levelname)s: %(message)s')
logger = logging.getLogger("crimes")

# Schéma de la table crimes
crime_schema = StructType([
    StructField("cmplnt_num", StringType()),
    StructField("addr_pct_cd", IntegerType()),
    StructField("boro_nm", StringType()),
    StructField("cmplnt_fr_dt", TimestampType()),
    StructField("cmplnt_fr_tm", StringType()),
    StructField("cmplnt_to_dt", TimestampType()),
    StructField("cmplnt_to_tm", StringType()),
    StructField("crm_atpt_cptd_cd", StringType()),
    StructField("hadevelopt", StringType()),
    StructField("housing_psa", IntegerType()),
    StructField("jurisdiction_code", IntegerType()),
    StructField("juris_desc", StringType()),
    StructField("ky_cd", IntegerType()),
    StructField("law_cat_cd", StringType()),
    StructField("loc_of_occur_desc", StringType()),
    StructField("ofns_desc", StringType()),
    StructField("parks_nm", StringType()),
    StructField("patrol_boro", StringType()),
    StructField("pd_cd", IntegerType()),
    StructField("pd_desc", StringType()),
    StructField("prem_typ_desc", StringType()),
    StructField("rpt_dt", TimestampType()),
    StructField("station_name", StringType()),
    StructField("susp_age_group", StringType()),
    StructField("susp_race", StringType()),
    StructField("susp_sex", StringType()),
    StructField("transit_district", IntegerType()),
    StructField("vic_age_group", StringType()),
    StructField("vic_race", StringType()),
    StructField("vic_sex", StringType()),
    StructField("x_coord_cd", IntegerType()),
    StructField("y_coord_cd", IntegerType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType())
])

# Schéma de la table crime_train
train_schema = StructType([
    StructField("id", StringType()),  # Référence vers cmplnt_num
    StructField("status", StringType())  # 'sent' ou null
])

# Paramètres de connexion PostgreSQL
jdbc_url = "jdbc:postgresql://postgres:5432/crimenyc"
db_props = {
    "user": "crimenyc",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# Création de la session Spark
spark = SparkSession.builder \
    .appName("InsertOnlyNewCrimesAndSendToKafka") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Création de la table crimes si elle n'existe pas
def ensure_table_exists():
    try:
        spark.read.jdbc(jdbc_url, "crimes", properties=db_props).limit(1).collect()
    except:
        spark.createDataFrame([], crime_schema).write.jdbc(jdbc_url, "crimes", mode="overwrite", properties=db_props)
        logger.warning("Table 'crimes' créée via Spark car elle était absente.")

# Création de la table crime_train si elle n'existe pas
def ensure_train_table_exists():
    try:
        spark.read.jdbc(jdbc_url, "crime_train", properties=db_props).limit(1).collect()
    except:
        spark.createDataFrame([], train_schema).write.jdbc(jdbc_url, "crime_train", mode="overwrite", properties=db_props)
        logger.warning("Table 'crime_train' créée via Spark car elle était absente.")

# Fonction pour convertir dynamiquement chaque colonne au bon type défini dans le schéma
def cast_columns_to_schema(df, schema):
    for field in schema.fields:
        if field.name not in df.columns:
            default_value = lit(0) if isinstance(field.dataType, (IntegerType, DoubleType)) else lit(None)
            df = df.withColumn(field.name, default_value.cast(field.dataType))
        else:
            try:
                if isinstance(field.dataType, TimestampType):
                    df = df.withColumn(field.name, to_timestamp(col(field.name)))
                else:
                    df = df.withColumn(field.name, col(field.name).cast(field.dataType))
            except Exception as e:
                logger.error(f"Erreur de conversion de la colonne {field.name} : {e}")
    return df.select([f.name for f in schema.fields])

def combine_dataframes(df_list):
    return reduce(DataFrame.unionAll, df_list)

# Traitement par batch
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    print(f"📥 Batch {batch_id} reçu avec {batch_df.count()} ligne(s)")
    ensure_table_exists()
    ensure_train_table_exists()

    # Lecture brute des données Kafka
    raw_rows = list({row["raw"]: row for row in batch_df.selectExpr("CAST(value AS STRING) AS raw").collect()}.values())
    new_kafka_rows = []
    new_df_list = []

    # Extraction des cmplnt_num depuis les messages Kafka (raw_rows)
    raw_ids = [json.loads(row["raw"]).get("cmplnt_num") for row in raw_rows if "cmplnt_num" in json.loads(row["raw"])]
    try:
        id_string = ",".join(["'{}'".format(id) for id in raw_ids if id])
        ids_query = f"SELECT cmplnt_num FROM \"crimes\" WHERE cmplnt_num IN ({id_string})"
        existing_df = spark.read.jdbc(jdbc_url, f"({ids_query}) as existing", properties=db_props)
        existing_ids = set(row["cmplnt_num"] for row in existing_df.collect())
        print(f"🔎 {len(existing_ids)} lignes déjà présentes dans la table crimes.")
    except Exception as e:
        logger.error(f"Erreur lecture table PostgreSQL : {e}")
        existing_ids = set()

    if not raw_rows:
        print("⚠️ Aucun message Kafka à traiter dans ce batch.")
    # Traitement des nouvelles données kafka (insertion dans crimes)
    for i, row in enumerate(raw_rows, 1):
        try:
            crime_json = json.loads(row["raw"])
            cmplnt_num = crime_json.get("cmplnt_num")
            if cmplnt_num and cmplnt_num not in existing_ids:
                temp_df = spark.read.json(spark.sparkContext.parallelize([json.dumps(crime_json)]))
                casted_df = cast_columns_to_schema(temp_df, crime_schema)
                new_df_list.append(casted_df)
            print(f"✅ Ligne {i}/{len(raw_rows)} traitée (casté - filtré)")
        except Exception as e:
            logger.error(f"Erreur de traitement d’un message : {e}")

    raw_rows = []
    new_kafka_rows = []
    existing_ids = set()

  # Insertion des nouveaux crimes détectés
    if new_df_list:
        print("combine dataframes")
        # Si on a plusieurs DataFrame, on les combine avec unionAll
        full_df = combine_dataframes(new_df_list)
        print("Insertion des nouveaux crimes détectés")
        # On insère toutes les nouvelles lignes dans la table PostgreSQL "crimes"
        full_df.write.jdbc(jdbc_url, "crimes", mode="append", properties=db_props)
        # Affichage du nombre de lignes insérées
        print(f"🗃️ {full_df.count()} nouvelles lignes insérées dans la table crimes.")

    # Sélection des crimes non encore envoyés à l'IA (via table crime_train)
    # Optimisation mémoire : exécuter une requête SQL directement dans PostgreSQL pour éviter le chargement complet
    query = """
        SELECT c.*
        FROM \"crimes\" c
        WHERE NOT EXISTS (
            SELECT 1 FROM \"crime_train\" t WHERE t.id = c.cmplnt_num
        )
    """
    
    new_df_list = []

    joined_df = spark.read.jdbc(jdbc_url, f"({query}) AS joined", properties=db_props)

    # Si on a au moins 900 lignes non envoyées, on les prépare pour Kafka
    if joined_df.count() >= 900:
        batch_to_send = joined_df
        kafka_df = batch_to_send.selectExpr("to_json(struct(*)) AS value")

        # Envoi au topic Kafka 'train-data'
        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "train-data") \
            .save()

        # Mise à jour ou insertion dans crime_train avec status='sent'
        sent_ids = batch_to_send.select("cmplnt_num").withColumnRenamed("cmplnt_num", "id").withColumn("status", lit("sent"))
        sent_ids.write.jdbc(jdbc_url, "crime_train", mode="append", properties=db_props)
        print(f"📤 {sent_ids.count()} lignes envoyées à l'IA et marquées 'sent'.")
    else:
        print(f"⏳ Moins de 900 lignes disponibles à envoyer à l'IA ({joined_df.count()} ligne(s) trouvée(s)). Attente du prochain batch...")

# Lecture en streaming depuis Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "send-data") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Application du traitement à chaque batch
query = kafka_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/crimes_final") \
    .start()

print("✅ Streaming initialisé : lecture depuis 'send-data', écriture vers 'train-data'.")
query.awaitTermination()
