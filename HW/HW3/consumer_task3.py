import time
from functools import wraps
from kafka import KafkaConsumer

def backoff(tries, sleep):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < tries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    print(f"Attempt {attempts}/{tries} failed: {e}")
                    if attempts < tries:
                        time.sleep(sleep)
            raise RuntimeError(f"All {tries} attempts failed")
        return wrapper
    return decorator

@backoff(tries=10, sleep=60)
def message_handler(value) -> None:
    print(value)
    # Add here for HTTP GET request and saving to DB

def create_consumer():
    print("Connecting to Kafka brokers")
    consumer = KafkaConsumer("kir-test-processed-t3",
                             group_id='itmo_group1',
                             bootstrap_servers='localhost:29092',
                             auto_offset_reset='earliest',
                             enable_auto_commit=True)

    for message in consumer:
        message_handler(message.value)
        print(message)

if __name__ == '__main__':
    create_consumer()
