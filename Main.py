from random import choice, randint, random,uniform
import datetime
from time import strftime
import time
from faker import Faker
from confluent_kafka import SerializingProducer
from simplejson import dump
import json


fake = Faker()

def generate_transactions():
    user = fake.simple_profile()

    return {
        "transactionId": fake.uuid4(),
        "productId": choice(['item1', 'item2', 'item3', 'item4', 'item5', 'item5', 'item6']),
        "productName": choice(['mobile', 'tablet', 'laptop', 'speaker', 'headphone', 'smartwatch']),
        "productCategory": choice(['electronic', 'fashion', 'grocery', 'home', 'beauty', 'sports']),
        "productPrice": round(uniform(10, 1000), 2),
        "productQuantity": randint(1, 10),
        "productBrand": choice(['apple', 'samsung', 'oneplus', 'bose', 'sony', 'mi']),
        "currency": choice(['USD', 'KES']),
        "customerId": user['username'],
        "transactionDate": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "paymentMethod": choice(['M-PESA', 'PESALINK', 'Credit_Card'])
    }

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    return

def main():
    topic = 'financial_transaction'
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092',
        
    })
    current_timer = datetime.datetime.now()

    while (datetime.datetime.now() - current_timer).seconds < 120:
        try:
            transactions = generate_transactions()
            transactions['totalAmountSpent'] = transactions['productPrice'] * transactions['productQuantity']

            print(transactions)

            producer.produce(topic=topic, key=transactions['transactionId'], value=json.dumps(transactions), on_delivery=delivery_report)
            producer.poll(0)

            time.sleep(5)

        except BufferError as e:
            print(f'Local producer queue is full ({len(producer)} messages awaiting delivery): try again\n')
            time.sleep(1)

        except Exception as e:
            print(e)




if __name__ == '__main__':
    main()

