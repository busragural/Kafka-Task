
# KAFKA&DOCKER TASKS


## Introduction
There are three separate tasks in this study. For the first task, Kafka was installed over Docker and Kafka Cli Commands were used to send and listen to messages.

For the second task, data was scraped with Python from the first page of https://scrapeme.live/shop/, saved in a file and Rest Api Service was written for this file. 

In the third task, the work is dockerized.

## Project Structure


    kafka
    ├── docker-compose.yml                   
    ├── Dockerfile
    ├── scraper.py
    ├── api.py                                    
    └── requirements.txt 

- **docker-compose.yml**: Docker Compose configuration file to orchestrate multiple Docker containers (Zookeeper, Kafka, and the producer-consumer service).
- **Dockerfile**: Dockerfile to build the Docker image for the producer-consumer service.
- **scraper.py**: Python script to scrape data from a website and send it to a Kafka topic.
- **api.py**: Flask API to serve the data stored in a JSON file.
- **requirements.txt**: Python dependencies.

## Steps

### Task 1 - Messaging
 
- Installed [Docker Desktop](https://www.docker.com/products/docker-desktop/).
- Created a project file.
```bash
  cd desktop
  mkdir kafka
  cd kafka
```
- Created a file called **docker-compose.yml**. Necessary configurations are made for zookeeper, kafka and producer_consumer.

**zookeeper:** The Zookeeper service manages Kafka brokers.

**kafka:** The Kafka service is used to process messages.

**producer_consumer:** The service that runs my Python application. This service both sends data to Kafka and runs the Flask API.

- Started all services defined by Docker Compose in detached mode.
```bash
  docker-compose up -d
```
- Created new topic called learn-topic.
```bash
docker exec -it kafka-learn kafka-topics --create --topic learn-topic --bootstrap-server localhost:9092
```

- Run the following command in the terminal to gain access to the Kafka container.

```bash
docker exec -it kafka-learn /bin/bash
```

- Run the following command in order to send message.
```bash
kafka-console-producer --topic learn-topic --bootstrap-server localhost:9092
```

- Wrote messages and entered.

- Opened the new terminal window.

- Run the following command in the terminal to gain access to the Kafka container.

```bash
docker exec -it kafka-learn /bin/bash
```

- Listened to the messages.

```bash
kafka-console-consumer --topic learn-topic --from-beginning --bootstrap-server localhost:9092
```
![Messaging](https://i.ibb.co/fYDKQCm/Ekran-Resmi-2024-07-11-22-03-43.png)

### Task 2 - Scraping

- Scraped data from the first page of [ScrapeMe](https://scrapeme.live/shop/) using **Python requests**.

```bash
def get_data():
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
```

- Parsed the incoming data using bs4.  
```bash
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
```

- Wrote the data in 1 second intervals and json format to the kafka topic.
```bash
producer.produce(TOPIC, key=str(time.time()), value=json.dumps(item), callback=delivery_report)
```
![Scraping](https://i.ibb.co/QrwsCB7/Ekran-Resmi-2024-07-12-01-32-14.png)

- Saved the data in a file.
```bash
with open('data.json', 'w', encoding='utf-8') as f:
  for item in data:
      json.dump(item, f)
      f.write("\n")
```

![Save the data](https://i.ibb.co/MM8swGG/Ekran-Resmi-2024-07-12-01-35-20.png)
- Wrote Rest Api Service for the data using Flask.

```bash
@app.route('/data', methods=['GET'])
def get_data():
    data = []
        with open('/home/user/Desktop/kafka/data.json', 'r', encoding='utf-8') as f:
            for line in f:
                json_data = json.loads(line)
                data.append(json_data)
                print(json_data)  
    return jsonify(data)
```

### Task 3 - Dockerize

- Created Dockerfile.

_A Dockerfile is a script containing a series of instructions on how to build a Docker image. It specifies the base image, the software and dependencies to be installed, configurations to be set, and commands to be run to assemble the final image._

- Created requirements.txt .
  
_List of Python dependencies required for the project._

- Created Docker image.
```bash
docker build -t my-dbrain-app .
```

- Viewed images.
```bash
docker images
```

- Run the image. 
```bash
docker run my-dbrain-app
```

## References

- [Amol Damodar](https://medium.com/@AmolDamodar/mac-kafka-setup-with-docker-34142681cfd7)

- [Serkan Eren](https://medium.com/devopsturkiye/apache-kafkaya-giriş-3399e5f33f8e)

- [Amber Kakkar](https://medium.com/@amberkakkar01/getting-started-with-apache-kafka-on-docker-a-step-by-step-guide-48e71e241cf2)

- [GeeksForGeeks](https://www.geeksforgeeks.org/kafka-producer-cli-tutorial)


- [FreeCodeCamp](https://www.freecodecamp.org/news/how-to-dockerize-a-flask-app/)


