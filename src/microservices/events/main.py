import json
import logging
import os
import signal
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError


TOPICS = {
    'movie': 'movie-events',
    'user': 'user-events',
    'payment': 'payment-events',
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
logger = logging.getLogger('events-service')
logging.getLogger('kafka').setLevel(logging.WARNING)


def env(name: str, default: str) -> str:
    return os.getenv(name, default).strip() or default


def kafka_brokers() -> list[str]:
    return [broker.strip() for broker in env('KAFKA_BROKERS', 'localhost:9092').split(',') if broker.strip()]


def connect_producer(brokers: list[str]) -> KafkaProducer:
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=brokers,
                key_serializer=lambda value: value.encode('utf-8'),
                value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode('utf-8'),
                acks=1,
            )
            producer.bootstrap_connected()
            return producer
        except KafkaError as error:
            logger.warning('Kafka producer is not ready, retrying: %s', error)
            time.sleep(3)


class EventService:
    def __init__(self, brokers: list[str]) -> None:
        self.brokers = brokers
        self.producer = connect_producer(brokers)
        self.stop_event = threading.Event()
        self.consumer_threads: list[threading.Thread] = []

    def start_consumers(self) -> None:
        for event_type, topic in TOPICS.items():
            thread = threading.Thread(
                target=self.consume,
                args=(event_type, topic),
                name=f'{event_type}-consumer',
                daemon=True,
            )
            thread.start()
            self.consumer_threads.append(thread)

    def consume(self, event_type: str, topic: str) -> None:
        while not self.stop_event.is_set():
            consumer = None
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=self.brokers,
                    group_id=f'cinemaabyss-events-service-{topic}',
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    consumer_timeout_ms=1000,
                    value_deserializer=lambda value: json.loads(value.decode('utf-8')),
                )
                logger.info('consumer for %s events started on topic %s', event_type, topic)

                while not self.stop_event.is_set():
                    for message in consumer:
                        logger.info(
                            'processed %s event from topic %s partition=%s offset=%s key=%s payload=%s',
                            event_type,
                            topic,
                            message.partition,
                            message.offset,
                            message.key.decode('utf-8') if message.key else '',
                            json.dumps(message.value, ensure_ascii=False),
                        )
                        if self.stop_event.is_set():
                            break
            except KafkaError as error:
                logger.warning('consumer for topic %s stopped, will retry: %s', topic, error)
                self.stop_event.wait(3)
            finally:
                if consumer is not None:
                    consumer.close()

    def publish(self, event_type: str, payload: object) -> dict[str, str]:
        topic = TOPICS[event_type]
        event_id = f'{time.time_ns()}-{uuid.uuid4().hex[:16]}'
        envelope = {
            'id': event_id,
            'type': event_type,
            'created_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            'payload': payload,
        }
        self.producer.send(topic, key=event_id, value=envelope).get(timeout=10)
        logger.info('published %s event id=%s to topic %s', event_type, event_id, topic)
        return {'status': 'success', 'event_id': event_id, 'topic': topic}

    def close(self) -> None:
        self.stop_event.set()
        self.producer.flush(timeout=5)
        self.producer.close(timeout=5)


class EventHandler(BaseHTTPRequestHandler):
    service: EventService

    def do_GET(self) -> None:
        if self.path == '/api/events/health':
            self.write_json(200, {'status': True})
        else:
            self.write_json(404, {'status': 'error', 'message': 'not found'})

    def do_POST(self) -> None:
        event_type = self.path.removeprefix('/api/events/')
        if self.path != f'/api/events/{event_type}' or event_type not in TOPICS:
            self.write_json(404, {'status': 'error', 'message': 'not found'})
            return

        try:
            content_length = int(self.headers.get('Content-Length', '0'))
            payload = json.loads(self.rfile.read(content_length))
        except (ValueError, json.JSONDecodeError):
            self.write_json(400, {'status': 'error', 'message': 'request body must be valid JSON'})
            return

        try:
            response = self.service.publish(event_type, payload)
        except KafkaError as error:
            logger.exception('failed to publish %s event: %s', event_type, error)
            self.write_json(502, {'status': 'error', 'message': 'cannot publish event to Kafka'})
            return

        self.write_json(201, response)

    def write_json(self, status: int, value: object) -> None:
        body = json.dumps(value, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, message_format: str, *args: object) -> None:
        logger.info('%s - %s', self.address_string(), message_format % args)


def main() -> None:
    brokers = kafka_brokers()
    service = EventService(brokers)
    EventHandler.service = service
    service.start_consumers()

    port = int(env('PORT', '8082'))
    server = ThreadingHTTPServer(('0.0.0.0', port), EventHandler)

    def shutdown(_signum: int, _frame: object) -> None:
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    logger.info('starting events service on port %s, kafka brokers=%s', port, ','.join(brokers))
    try:
        server.serve_forever()
    finally:
        server.server_close()
        service.close()


if __name__ == '__main__':
    main()
