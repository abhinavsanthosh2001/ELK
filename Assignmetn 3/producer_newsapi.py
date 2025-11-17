"""
Real-time data producer that reads from NewsAPI and writes to Kafka.
This script continuously fetches news articles and streams them to Kafka topic1.
Alternative to Reddit producer when Reddit API is not available.
"""

import json
import time
import os
from confluent_kafka import Producer
from newsapi import NewsApiClient
from datetime import datetime, timedelta, timezone

# Configuration - can be set via environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'topic1')
NEWS_API_KEY = os.getenv('NEWS_API_KEY', '')
NEWS_CATEGORY = os.getenv('NEWS_CATEGORY', 'technology')  # technology, business, science, etc.
FETCH_INTERVAL = int(os.getenv('FETCH_INTERVAL', '60'))  # seconds between fetches

def create_kafka_producer():
    """Create and return a Kafka producer."""
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'newsapi-producer'
    }
    return Producer(config)

def create_newsapi_client():
    """Create and return a NewsAPI client."""
    if not NEWS_API_KEY:
        raise ValueError(
            "Please set NEWS_API_KEY environment variable. "
            "You can get a free API key from https://newsapi.org/register"
        )
    
    return NewsApiClient(api_key=NEWS_API_KEY)

def stream_news_articles(newsapi, producer, category, topic):
    """Stream news articles to Kafka."""
    print(f"Starting to stream {category} news articles to Kafka topic: {topic}")
    print(f"Fetching new articles every {FETCH_INTERVAL} seconds...")
    
    # For time-based analysis, we want continuous data flow
    # Remove duplicate tracking to allow re-processing articles
    
    try:
        while True:
            try:
                # Get top headlines for the category
                # Using 'us' as country, you can change this or use 'sources' parameter
                top_headlines = newsapi.get_top_headlines(
                    category=category,
                    language='en',
                    country='us',
                    page_size=100
                )
                
                if top_headlines['status'] == 'ok':
                    articles = top_headlines.get('articles', [])
                    new_count = 0
                    
                    for article in articles:
                        # Use URL as unique identifier
                        article_id = article.get('url', '')
                        
                        if article_id:  # Send all articles for continuous analysis
                            # Combine title and description for text content
                            title = article.get('title', '')
                            description = article.get('description', '')
                            content = article.get('content', '')
                            
                            # Combine all text fields
                            text_content = f"{title}\n{description}\n{content}".strip()
                            
                            if text_content:  # Only send if there's actual content
                                # Prepare message with article data
                                message = {
                                    'text': text_content,
                                    'title': title,
                                    'source': article.get('source', {}).get('name', 'Unknown'),
                                    'author': article.get('author', 'Unknown'),
                                    'category': category,
                                    'timestamp': datetime.now(timezone.utc).isoformat(),
                                    'article_id': article_id,
                                    'published_at': article.get('publishedAt', '')
                                }
                                
                                # Send to Kafka
                                producer.produce(
                                    topic,
                                    key=article_id.encode('utf-8') if article_id else None,
                                    value=json.dumps(message).encode('utf-8'),
                                    callback=lambda err, msg: None  # Optional callback
                                )
                                producer.poll(0)  # Trigger delivery callbacks
                                new_count += 1
                                print(f"Sent article: {title[:50]}...")
                    
                    print(f"Fetched {len(articles)} articles, {new_count} articles sent to Kafka")
                else:
                    print(f"Error from NewsAPI: {top_headlines.get('message', 'Unknown error')}")
                
                # Wait before next fetch
                time.sleep(FETCH_INTERVAL)
                
            except Exception as e:
                print(f"Error fetching articles: {e}")
                time.sleep(FETCH_INTERVAL)  # Wait before retrying
                continue
                
    except KeyboardInterrupt:
        print("\nStopping producer...")
    except Exception as e:
        print(f"Fatal error: {e}")

def main():
    """Main function to run the producer."""
    print("Initializing NewsAPI to Kafka Producer...")
    
    # Validate environment
    if not NEWS_API_KEY:
        print("ERROR: NEWS_API_KEY must be set!")
        print("\nTo get a NewsAPI key:")
        print("1. Go to https://newsapi.org/register")
        print("2. Sign up for a free account")
        print("3. Copy your API key from the dashboard")
        print("4. Set it as an environment variable:")
        print("   $env:NEWS_API_KEY='7cc052b5d09f450697aeeb6d082b592d'")
        print("\nNote: Free tier allows 100 requests per day")
        return
    
    try:
        # Create clients
        producer = create_kafka_producer()
        newsapi = create_newsapi_client()
        
        # Test NewsAPI connection
        print(f"Connected to NewsAPI. Streaming {NEWS_CATEGORY} news")
        
        # Start streaming
        stream_news_articles(newsapi, producer, NEWS_CATEGORY, KAFKA_TOPIC)
        
    except Exception as e:
        print(f"Fatal error: {e}")
    finally:
        if 'producer' in locals():
            producer.flush()  # Wait for all messages to be delivered
        print("Producer closed.")

if __name__ == "__main__":
    main()

