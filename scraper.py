import requests
from bs4 import BeautifulSoup
import json
import time
from confluent_kafka import Producer

url = "https://scrapeme.live/shop/"
TOPIC = "learn-topic"

conf = {
    'bootstrap.servers': 'localhost:29092',
    'client.id': 'python-producer'
}
producer = Producer(**conf)

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def get_data():
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        print(f"Failed to retrieve data. HTTP Status code: {response.status_code}")
        return None

def get_product_details(product_url):
    response = requests.get(product_url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        description = soup.select_one('.woocommerce-product-details__short-description').text.strip()
        stock = soup.select_one('.stock').text.strip()
        return description, stock
    else:
        print(f"Failed to retrieve product details. HTTP Status code: {response.status_code}")
        return None, None

def parse_data(html):
    soup = BeautifulSoup(html, 'html.parser')
    products = []
    
    for product in soup.select('.product'):
        name = product.select_one('.woocommerce-loop-product__title').text.strip()
        price = product.select_one('.price').text.strip()
        product_link = product.find('a', class_='woocommerce-LoopProduct-link')['href']
        
        description, stock = get_product_details(product_link)
        
        products.append({
            'name': name,
            'price': price,
            'description': description,
            'stock': stock
        })
    
    return products

def main():
    html = get_data()
    if html:
        data = parse_data(html)
        with open('data.json', 'w', encoding='utf-8') as f:
            for item in data:
                producer.produce(TOPIC, key=str(time.time()), value=json.dumps(item), callback=delivery_report)
                json.dump(item, f)
                f.write("\n")
                time.sleep(1)
            producer.flush()

if __name__ == "__main__":
    main()
