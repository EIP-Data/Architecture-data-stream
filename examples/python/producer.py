#!/usr/bin/env python3
"""
Kafka Producer Example
Produces messages to Kafka and stores metadata in Cassandra
"""

import json
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


class DataStreamProducer:
    def __init__(self, kafka_servers=['localhost:9092'], cassandra_nodes=['localhost']):
        # Initialize Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )

        # Initialize Cassandra Connection
        self.cluster = Cluster(cassandra_nodes)
        self.session = None
        self._init_cassandra()

    def _init_cassandra(self):
        """Initialize Cassandra keyspace and table"""
        print("Connecting to Cassandra...")
        max_retries = 10
        for i in range(max_retries):
            try:
                self.session = self.cluster.connect()
                break
            except Exception as e:
                if i < max_retries - 1:
                    print(f"Cassandra not ready yet, retrying... ({i+1}/{max_retries})")
                    time.sleep(5)
                else:
                    raise e

        # Create keyspace if it doesn't exist
        self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS datastream
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
        """)

        # Use the keyspace
        self.session.set_keyspace('datastream')

        # Create table for message metadata
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS message_metadata (
                message_id uuid PRIMARY KEY,
                topic text,
                timestamp timestamp,
                partition int,
                offset bigint,
                key text,
                value_size int
            )
        """)
        print("Cassandra initialized successfully!")

    def produce_message(self, topic, key, value):
        """Send a message to Kafka and store metadata in Cassandra"""
        message_id = uuid.uuid4()
        timestamp = datetime.now()

        # Send to Kafka
        future = self.producer.send(topic, key=key, value=value)

        try:
            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)

            # Store metadata in Cassandra
            self.session.execute("""
                INSERT INTO message_metadata (message_id, topic, timestamp, partition, offset, key, value_size)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                message_id,
                topic,
                timestamp,
                record_metadata.partition,
                record_metadata.offset,
                key,
                len(json.dumps(value))
            ))

            print(f"Message sent - ID: {message_id}, Topic: {topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            return message_id

        except Exception as e:
            print(f"Error sending message: {e}")
            return None

    def close(self):
        """Close connections"""
        self.producer.flush()
        self.producer.close()
        self.cluster.shutdown()


def main():
    print("Starting Kafka Producer...")

    # Initialize producer
    producer = DataStreamProducer()

    topic = 'user-events'

    # Produce sample messages
    try:
        for i in range(10):
            message = {
                'event_id': str(uuid.uuid4()),
                'user_id': f'user_{i % 5}',
                'event_type': ['login', 'purchase', 'view', 'logout'][i % 4],
                'timestamp': datetime.now().isoformat(),
                'data': {
                    'browser': 'Chrome',
                    'os': 'Linux',
                    'value': i * 100
                }
            }

            producer.produce_message(
                topic=topic,
                key=f'user_{i % 5}',
                value=message
            )

            time.sleep(1)  # Wait 1 second between messages

        print("\nAll messages sent successfully!")

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        producer.close()


if __name__ == '__main__':
    main()
