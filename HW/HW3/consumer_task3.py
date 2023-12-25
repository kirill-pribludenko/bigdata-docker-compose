import time
from functools import wraps
from kafka import KafkaConsumer
import random


def fake_site_connect():
    list_of_http_status = [200]*4 + [400]
    if random.choice(list_of_http_status) == 400:
        raise Exception("Site connection error!")
    else:
        urgent_data = "Warning!!! "
        return urgent_data


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


@backoff(tries=3, sleep=10)
def message_handler(message):
    data = fake_site_connect()
    new_message = data + str(message)
    print(new_message)


def create_consumer():
    print("Connecting to Kafka brokers")
    consumer = KafkaConsumer("kir-test-processed-t3",
                             group_id='itmo_group',
                             bootstrap_servers='localhost:29092',
                             auto_offset_reset='earliest',
                             enable_auto_commit=True)

    for message in consumer:
        message_handler(message)


if __name__ == '__main__':
    create_consumer()
