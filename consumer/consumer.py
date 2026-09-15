from confluent_kafka import Consumer, Producer
import psycopg2
import json
import time
import os 


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fraud")
POSTGRES_USER = os.getenv("POSTGRES_USER", "fraud_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "fraud_pass")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:29092"
)

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "fraud-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

consumer.subscribe(["transactions"])

dlq_producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})

def connect_postgres():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

    cursor = conn.cursor()
    return conn, cursor

conn, cursor = connect_postgres()

print("Waiting for transactions...")

while True:
    message = consumer.poll(1.0)

    if message is None:
        continue

    if message.error():
        print("Kafka error:", message.error())
        continue

    event = json.loads(message.value().decode("utf-8"))

    print("Receive event:")
    print(event)

    max_retries = 3
    retry_count = 0

    while True:
        try:
            cursor.execute(
                """
                INSERT INTO fraud_predictions (
                    event_id,
                    customer_id,
                    amount,
                    merchant,
                    hour,
                    risk_score,
                    decision
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    event["event_id"],
                    event["customer_id"],
                    event["amount"],
                    event["merchant"],
                    event["hour"],
                    event["risk_score"],
                    event["decision"]
                )
            )

            conn.commit()

            consumer.commit(
                message=message,
                asynchronous=False
            )

            print("Saved to PostgreSQL and offset committed")

            break

        except psycopg2.Error as e:
            print("PostgreSQL error:", e)

            try:
                conn.rollback()
            except psycopg2.Error:
                pass

            print("Trying to reconnect to PostgreSQL...")

            while True:
                try:
                    conn, cursor = connect_postgres()
                    print("Reconnected to PostgreSQL")
                    break

                except psycopg2.Error as reconnect_error:
                    print("PostgreSQL still unavailable:", reconnect_error)
                    time.sleep(3)

        except (KeyError, TypeError, ValueError) as e:
            retry_count += 1

            print(
                f"Processing error. "
                f"Attempt {retry_count}/{max_retries}: {e}"
            )

            if retry_count >= max_retries:
                dlq_event = {
                    "original_event": event,
                    "error": str(e)
                }

                dlq_producer.produce(
                    "transactions-dlq",
                    value=json.dumps(dlq_event)
                )

                dlq_producer.flush()

                consumer.commit(
                    message=message,
                    asynchronous=False
                )

                print("Event sent to DLQ and offset committed")
                break