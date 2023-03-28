from fastapi import FastAPI
from pydantic import BaseModel

import uvicorn
import aiokafka
import asyncio
import json
import logging
import os

app = FastAPI()

flight_consumer = None
flight_consumer_task = None
response_consumer = None
response_consumer_task = None
booking_producer = None

available_flights = []

KAFKA_BOOKING_TOPIC = os.getenv('KAFKA_BOOKING_TOPIC', "booking")
KAFKA_RESPONSE_TOPIC = os.getenv('KAFKA_RESPONSE_TOPIC', "response")
KAFKA_FLIGHT_TOPIC = os.getenv('KAFKA_FLIGHT_TOPIC', "flight")
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP_PREFIX', 'service_group')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')


logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
log = logging.getLogger(__name__)


class Booking(BaseModel):
    booking_id: int
    number_of_tickets: int
    flight_id: int


@app.on_event("startup")
async def startup_event():
    log.info('Initializing API ...')
    await initialize()
    await consume()


@app.on_event("shutdown")
async def shutdown_event():
    log.info('Shutting down API')
    flight_consumer_task.cancel()
    response_consumer_task.cancel()
    await flight_consumer.stop()
    await booking_producer.stop()
    await response_consumer.stop()


@app.get("/")
async def root():
    return {"message": "Customer service running"}


@app.get("/all_flights")
async def get_flights():
    return available_flights


@app.post("/create_booking")
async def create_booking(booking: Booking):
    value_json = json.dumps(booking.__dict__).encode('utf-8')
    await booking_producer.send_and_wait(KAFKA_BOOKING_TOPIC, value_json)
    return booking


async def initialize():
    global flight_consumer
    group_id = f'{KAFKA_CONSUMER_GROUP}-1'
    log.debug(f'Initializing KafkaConsumer for topic {KAFKA_FLIGHT_TOPIC}, group_id {group_id}'
              f' and using bootstrap servers {KAFKA_BOOTSTRAP_SERVERS}')
    flight_consumer = aiokafka.AIOKafkaConsumer(KAFKA_FLIGHT_TOPIC,
                                                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                                group_id=group_id)
    await flight_consumer.start()

    global response_consumer
    log.debug(f'Initializing KafkaConsumer for topic {KAFKA_RESPONSE_TOPIC}, group_id {group_id}'
              f' and using bootstrap servers {KAFKA_BOOTSTRAP_SERVERS}')
    response_consumer = aiokafka.AIOKafkaConsumer(KAFKA_RESPONSE_TOPIC,
                                                     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                                     group_id=group_id)
    await response_consumer.start()

    global booking_producer
    booking_producer = aiokafka.AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await booking_producer.start()


async def consume():
    global flight_consumer_task
    global response_consumer_task
    flight_consumer_task = asyncio.create_task(consume_flights(flight_consumer))
    response_consumer_task = asyncio.create_task(consume_booking_response(response_consumer))


async def consume_flights(consumer):
    try:
        async for msg in consumer:
            log.info(f"Message consumed: {msg}")
            flight = json.loads(msg.value)
            exists = next((x for x in available_flights if x["flight_id"] == flight["flight_id"]), None)
            if exists is None:
                available_flights.append(flight)
            else:
                exists["status"] = flight["status"]
            message = f'Flight {flight["flight_id"]} with {flight["number_of_seats"]} seats is {flight["status"]}'
            print(message)
    finally:
        log.warning('Stopping consumer')
        await consumer.stop()


async def consume_booking_response(consumer):
    try:
        async for msg in consumer:
            log.info(f"Message consumed: {msg}")
            response = json.loads(msg.value)
            message = f'Your booking {response["status"]}'
            if response["status"] == "is approved":
                exists = next((x for x in available_flights if x["flight_id"] == response["flight_id"]), None)
                exists["number_of_seats"] = response["number_of_seats"]
            print(message)
    finally:
        log.warning('Stopping consumer')
        await consumer.stop()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8002)

