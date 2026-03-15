#!/usr/bin/env python3
"""
Kafka Consumer Example
Consumes messages from Kafka and stores them in Cassandra
"""

import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from cassandra.cluster import Cluster


class DataStreamConsumer:
    def __init__(self, kafka_servers=['localhost:9092'], cassandra_nodes=['localhost'], group_id='python-consumer-group'):
        # Initialize Kafka Consumer
        self.consumer = KafkaConsumer(
            bootstrap_servers=kafka_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None
        )

        # Initialize Cassandra Connection
        self.cluster = Cluster(cassandra_nodes)
        self.session = None
        self._init_cassandra()

    def _init_cassandra(self):
        """Initialize Cassandra connection and tables"""
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

        # Create table for consumed events
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS user_events (
                event_id uuid PRIMARY KEY,
                user_id text,
                event_type text,
                timestamp timestamp,
                browser text,
                os text,
                value int,
                consumed_at timestamp
            )
        """)

        # Create table for user event counts
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS user_event_counts (
                user_id text PRIMARY KEY,
                total_events counter
            )
        """)

        print("Cassandra initialized successfully!")

    def subscribe(self, topics):
        """Subscribe to Kafka topics"""
        self.consumer.subscribe(topics)
        print(f"Subscribed to topics: {topics}")

    def store_event(self, message):
        """Store consumed event in Cassandra"""
        try:
            value = message.value

            # Extract timestamp from message or use current time
            timestamp_str = value.get('timestamp')
            if timestamp_str:
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                timestamp = datetime.now()

            # Insert event into user_events table
            event_id = value.get('event_id')
            user_id = value.get('user_id')

            self.session.execute("""
                INSERT INTO user_events (event_id, user_id, event_type, timestamp, browser, os, value, consumed_at)
                VALUES (uuid(), %s, %s, %s, %s, %s, %s, toTimestamp(now()))
            """, (
                user_id,
                value.get('event_type'),
                timestamp,
                value.get('data', {}).get('browser'),
                value.get('data', {}).get('os'),
                value.get('data', {}).get('value')
            ))

            # Update counter for user
            self.session.execute("""
                UPDATE user_event_counts
                SET total_events = total_events + 1
                WHERE user_id = %s
            """, (user_id,))

            print(f"Stored event for user {user_id} - Type: {value.get('event_type')}")

        except Exception as e:
            print(f"Error storing event: {e}")

    def consume(self):
        """Start consuming messages"""
        print("\nStarting to consume messages... (Press Ctrl+C to stop)")
        try:
            for message in self.consumer:
                print(f"\n--- New Message ---")
                print(f"Topic: {message.topic}")
                print(f"Partition: {message.partition}")
                print(f"Offset: {message.offset}")
                print(f"Key: {message.key}")
                print(f"Value: {json.dumps(message.value, indent=2)}")

                # Store in Cassandra
                self.store_event(message)

        except KeyboardInterrupt:
            print("\nShutting down...")
        finally:
            self.close()

    def get_user_stats(self, user_id):
        """Get statistics for a user from Cassandra"""
        result = self.session.execute("""
            SELECT total_events FROM user_event_counts WHERE user_id = %s
        """, (user_id,))

        row = result.one()
        if row:
            return row.total_events
        return 0

    def close(self):
        """Close connections"""
        self.consumer.close()
        self.cluster.shutdown()


def main():
    print("Starting Kafka Consumer...")

    # Initialize consumer
    consumer = DataStreamConsumer()

    # Subscribe to topics
    consumer.subscribe(['user-events'])

    # Start consuming
    consumer.consume()


if __name__ == '__main__':
    main()
