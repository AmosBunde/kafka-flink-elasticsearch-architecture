from random import choice, randint
from time import strftime
from faker import Faker
from confluent_kafka import SerializingProducer


fake = Faker()

def genarate_transactions():
    user = fake.simple_profile()

    return {
        "transactionId": fake.uuid4(),
        "productId": random.choice(['item1','item2','item3','item4','item5','item5','item6']),
        "productName": random.choice(['mobile','tablet','laptop','speaker','headphone','smartwatch']),
        "productCategory": random.choice(['electronic','fashion','grocery','home','beauty','sports']),
        "productPrice": round(random.uniform( a: 1, b: 10),2),
        "productQuantity": random.randint( a: 1, b: 10),
        "productBrand": random.choice(['apple','samsung','oneplus','bose','sony','mi']),
        "currency": random.choice(['USD','KES']),
        "customerId": user['username'],
        "transactionDate": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "paymentMethod" : random.choice(['M-PESA','PESALINK','Credit_Card'])
    }