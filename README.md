# Real-Time Named Entity Recognition with Kafka and Spark

A streaming data pipeline that performs Named Entity Recognition (NER) on news articles using Apache Kafka, Apache Spark, and spaCy.

## Architecture

```
NewsAPI → Kafka (topic1) → Spark Streaming + spaCy NER → Kafka (topic2) → Logstash → Elasticsearch → Kibana
```

## Prerequisites

- Docker & Docker Compose
- Python 3.12+
- Java 11+

## Quick Start

### 1. Setup Environment

```bash
# Create virtual environment
python -m venv venv_sparknlp
source venv_sparknlp/bin/activate  # Linux/WSL
# or
venv_sparknlp\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt

# Download spaCy model
python -m spacy download en_core_web_sm
```

### 2. Start Kafka with Docker

```bash
# Start Kafka cluster
docker-compose up -d

# Create topics
docker exec -it kafka kafka-topics --create --topic topic1 --bootstrap-server localhost:9092
docker exec -it kafka kafka-topics --create --topic topic2 --bootstrap-server localhost:9092
```

### 3. Set NewsAPI Key

```bash
# Get free API key from https://newsapi.org/register
export NEWS_API_KEY="your_api_key_here"
```

### 4. Run the Pipeline

```bash
# Terminal 1: Start news producer
python producer_newsapi.py

# Terminal 2: Start Spark streaming
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 spark_streaming.py

# Terminal 3: Start Logstash (optional)
logstash -f logstash.conf
```

## Files

- `spark_streaming.py` - Main Spark application with spaCy NER
- `producer_newsapi.py` - NewsAPI to Kafka producer
- `docker-compose.yml` - Kafka cluster setup
- `logstash.conf` - Logstash configuration
- `requirements.txt` - Python dependencies

## Environment Variables

- `NEWS_API_KEY` - NewsAPI key (required)
- `KAFKA_BOOTSTRAP_SERVERS` - Default: localhost:9092
- `KAFKA_INPUT_TOPIC` - Default: topic1
- `KAFKA_OUTPUT_TOPIC` - Default: topic2

## Output Format

Topic2 receives JSON messages with entity counts:
```json
{"entity_name": "Apple_ORG", "count": 15}
{"entity_name": "New York_GPE", "count": 8}
```

## Troubleshooting

- Ensure Kafka is running: `docker ps`
- Check topic creation: `docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092`
- Monitor logs: `docker logs kafka`