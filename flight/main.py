from fastapi import FastAPI
from pydantic import BaseModel

import uvicorn
import aiokafka
import logging
import asyncio
import os
import json


app = FastAPI()

booking_consumer = None
flight_consumer = None
response_producer = None
flight_consumer_task = None
booking_consumer_task = None
flights = []
bookings = []


KAFKA_BOOKING_TOPIC = os.getenv('KAFKA_BOOKING_TOPIC', "booking")
KAFKA_RESPONSE_TOPIC = os.getenv('KAFKA_RESPONSE_TOPIC', "response")
KAFKA_FLIGHT_TOPIC = os.getenv('KAFKA_FLIGHT_TOPIC', "flight")
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP_PREFIX', 'service_group')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
log = logging.getLogger(__name__)


class Response(BaseModel):
    booking_id: int
    flight_id: int
    status: str
    number_of_seats: int


@app.on_event("startup")
async def startup_event():
    log.info('Initializing API ...')
    await initialize()
    await consume()


@app.on_event("shutdown")
async def shutdown_event():
    log.info('Shutting down API')
    flight_consumer_task.cancel()
    booking_consumer_task.cancel()
    await flight_consumer.stop()
    await booking_consumer.stop()
    await response_producer.stop()


@app.get("/")
async def root():
    return {"message": "Flight service running"}


async def initialize():
    global flight_consumer
    group_id = f'{KAFKA_CONSUMER_GROUP}'
    log.debug(f'Initializing KafkaConsumer for topic {KAFKA_FLIGHT_TOPIC}, group_id {group_id}'
              f' and using bootstrap servers {KAFKA_BOOTSTRAP_SERVERS}')
    flight_consumer = aiokafka.AIOKafkaConsumer(KAFKA_FLIGHT_TOPIC,
                                                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                                group_id=group_id)
    await flight_consumer.start()

    global booking_consumer
    log.debug(f'Initializing KafkaConsumer for topic {KAFKA_BOOKING_TOPIC}, group_id {group_id}'
              f' and using bootstrap servers {KAFKA_BOOTSTRAP_SERVERS}')
    booking_consumer = aiokafka.AIOKafkaConsumer(KAFKA_BOOKING_TOPIC,
                                                  bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                                  group_id=group_id)
    await booking_consumer.start()

    global response_producer
    response_producer = aiokafka.AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await response_producer.start()


async def consume():
    global flight_consumer_task
    global booking_consumer_task
    flight_consumer_task = asyncio.create_task(consume_flights(flight_consumer))
    booking_consumer_task = asyncio.create_task(consume_bookings(booking_consumer))


async def consume_flights(consumer):
    try:
        async for msg in consumer:
            log.info(f"Message consumed: {msg}")
            flight = json.loads(msg.value)
            exists = next((x for x in flights if x["flight_id"] == flight["flight_id"]), None)
            if exists is None:
                flights.append(flight)
            else:
                exists["status"] = flight["status"]
            message = f'Flight {flight["flight_id"]} with {flight["number_of_seats"]} seats is {flight["status"]}'
            print(message)
    finally:
        log.warning('Stopping consumer')
        await consumer.stop()


async def consume_bookings(consumer):
    try:
        async for msg in consumer:
            log.info(f"Message consumed: {msg}")
            booking = json.loads(msg.value)
            message = f'New booking {booking["booking_id"]} for a flight {booking["flight_id"]} and {booking["number_of_tickets"]} tickets'
            print(message)
            exists = next((x for x in flights if x["flight_id"] == booking["flight_id"]), None)
            status = "is not approved"
            response = Response(booking_id=booking["booking_id"], flight_id=booking["flight_id"], status=status, number_of_seats=0)
            if exists is None:
                response.status = "is not approved. Such flight id does not exist"
            else:
                if booking["number_of_tickets"] > exists["number_of_seats"]:
                    response.status = f'is not approved. There are only {exists["number_of_seats"]} seats available'
                elif booking["number_of_tickets"] <= exists["number_of_seats"] and booking["number_of_tickets"] > 0:
                    exists["number_of_seats"] -= booking["number_of_tickets"]
                    response.status = "is approved"
                    response.number_of_seats = exists["number_of_seats"]
                    bookings.append(booking)
            await response_producer.send_and_wait(KAFKA_RESPONSE_TOPIC, json.dumps(response.__dict__).encode('utf-8'))
    finally:
        log.warning('Stopping consumer')
        await consumer.stop()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8001)
