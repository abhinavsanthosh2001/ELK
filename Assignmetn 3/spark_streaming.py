import os
import json
import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, to_json, struct, udf
)
from pyspark.sql.types import StringType, ArrayType

# -------------------------------
# 1️⃣ Load spaCy model
# -------------------------------
nlp = spacy.load("en_core_web_sm")

# ---------------------------------
# 2️⃣ NER extraction from JSON input
# ---------------------------------
def extract_entities_spacy(msg):
    """Parses Kafka JSON, extracts .text, performs NER."""
    if not msg:
        return []

    # Try to parse JSON
    try:
        data = json.loads(msg)
        article_text = data.get("text", "")
    except json.JSONDecodeError:
        # fallback if message is not JSON
        article_text = msg

    if not article_text:
        return []

    # spaCy extraction
    doc = nlp(article_text)
    return [f"{ent.text}_{ent.label_}" for ent in doc.ents]

extract_entities_udf = udf(extract_entities_spacy, ArrayType(StringType()))

# -------------------------------
# 3️⃣ Initialize Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("NamedEntityCountStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# 4️⃣ Kafka configuration
# -------------------------------
kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
input_topic = os.getenv("KAFKA_INPUT_TOPIC", "topic1")
output_topic = os.getenv("KAFKA_OUTPUT_TOPIC", "topic2")

# -------------------------------
# 5️⃣ Read JSON news from Kafka
# -------------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

messages_df = df.selectExpr("CAST(value AS STRING) AS json_string")

# -------------------------------
# 6️⃣ Extract entities from .text
# -------------------------------
entities_df = messages_df.withColumn(
    "entities",
    extract_entities_udf(col("json_string"))
)

# Explode → one row per entity string
exploded_df = entities_df.select(explode(col("entities")).alias("entity"))

# -------------------------------
# 7️⃣ Global running counts
# -------------------------------
running_count_df = exploded_df.groupBy("entity").count()

# -------------------------------
# 8️⃣ Format output JSON for Kafka
# -------------------------------
kafka_output_df = running_count_df.select(
    to_json(
        struct(
            col("entity").alias("entity_name"),
            col("count")
        )
    ).alias("value")
)

# -------------------------------
# 9️⃣ Write aggregated counts to Kafka topic2
# -------------------------------
query = kafka_output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", output_topic) \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .outputMode("update") \
    .trigger(processingTime="60 seconds") \
    .start()

query.awaitTermination()
