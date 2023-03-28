from fastapi import FastAPI
from pydantic import BaseModel

import uvicorn
import aiokafka
import logging
import os
import json


app = FastAPI()

flight_producer = None

KAFKA_FLIGHT_TOPIC = os.getenv('KAFKA_FLIGHT_TOPIC', "flight")
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
log = logging.getLogger(__name__)


class Flight(BaseModel):
    flight_id: int
    number_of_seats: int
    status: str


@app.on_event("startup")
async def startup_event():
    log.info('Initializing API ...')
    await initialize()


@app.on_event("shutdown")
async def shutdown_event():
    log.info('Shutting down API')
    await flight_producer.stop()


@app.get("/")
async def root():
    return {"message": "Airline service running"}


@app.post("/create_flight")
async def create_flight(flight: Flight):
    if flight.number_of_seats > 0:
        value_json = json.dumps(flight.__dict__).encode('utf-8')
        await flight_producer.send_and_wait(KAFKA_FLIGHT_TOPIC, value_json)
        return flight


async def initialize():
    global flight_producer
    flight_producer = aiokafka.AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await flight_producer.start()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
