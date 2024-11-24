from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Créer une session Spark avec le package Kafka
spark = SparkSession.builder \
    .appName("DataIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
    .getOrCreate()

# Charger les fichiers statiques depuis le dossier 'input_data'
population_df = spark.read.csv("/mnt/c/Users/Mahamat Adam/OneDrive/Documents/Cours 2023-2024/S2/Data integration/projet/project-data-integration/input_data/aggregate-household-income-in-the-past-12-months-in-2015-inflation-adjusted-dollars.csv", header=True, inferSchema=True)
insurance_df = spark.read.csv("/mnt/c/Users/Mahamat Adam/OneDrive/Documents/Cours 2023-2024/S2/Data integration/projet/project-data-integration/input_data/self-employment-income-in-the-past-12-months-for-households.csv", header=True, inferSchema=True)
employment_df = spark.read.csv("/mnt/c/Users/Mahamat Adam/OneDrive/Documents/Cours 2023-2024/S2/Data integration/projet/project-data-integration/input_data/self-employment-income-in-the-past-12-months-for-households.csv", header=True, inferSchema=True)

population_df = population_df.withColumn("Id", population_df["Id"].cast("string"))
insurance_df = insurance_df.withColumn("Id", insurance_df["Id"].cast("string"))
employment_df = employment_df.withColumn("Id", employment_df["Id"].cast("string"))

# structure du message Kafka attendu (ici un JSON avec une colonne 'Id')
kafka_schema = StructType([
    StructField("Id", StringType(), True),
    StructField("some_other_column", StringType(), True)  # Ajouter d'autres colonnes si nécessaire
])

# Connexion au stream Kafka avec l'API de streaming de DataFrame
kafka_broker = "localhost:9092"
topic = "population_data"

# Lire les flux Kafka
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic) \
    .load()


kafka_stream_df = kafka_stream_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Traiter les données du Kafka stream
def process_stream(kafka_stream_df):
    # Convertir le flux Kafka en DataFrame JSON
    streaming_df = kafka_stream_df.selectExpr("value as json").select(from_json("json", kafka_schema).alias("data")).select("data.*")
    
    # Effectuer les jointures avec les DataFrames statiques
    joined_df = streaming_df \
        .join(population_df, on='Id', how='inner') \
        .join(insurance_df, on='Id', how='inner') \
        .join(employment_df, on='Id', how='inner')

    # Retourner le DataFrame joint
    return joined_df

# Appliquer le traitement au flux
processed_df = process_stream(kafka_stream_df)

# Écrire le résultat en continu dans Parquet
query = processed_df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/mnt/c/Users/Mahamat Adam/OneDrive/Documents/Cours 2023-2024/S2/Data integration/projet/project-data-integration/output_data/integrated_data") \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .start()

# Attendre la terminaison du processus
query.awaitTermination()
