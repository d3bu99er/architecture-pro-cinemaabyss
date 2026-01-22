import os
import json
import logging
import asyncio
import threading  # ✅ Добавлен для синхронного Lock
from datetime import datetime
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, Request, Response, status
from fastapi.responses import JSONResponse
from aiokafka import AIOKafkaProducer
import uuid


# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


app = FastAPI(title="Events Service MVP")


# Глобальные переменные ✅ ИСПРАВЛЕНО: threading.Lock для синхронного кода
kafka_producer: Optional[AIOKafkaProducer] = None
events_store: List[Dict[str, Any]] = []
event_counter = 0
store_lock = asyncio.Lock()  # Для асинхронного кода
counter_lock = threading.Lock()  # ✅ Для синхронного конструктора Event

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092").split(",")
PORT = int(os.getenv("PORT", "8082"))


class Event:
    def __init__(self, event_type: str, data: Dict[str, Any]):
        global event_counter
        # ✅ Теперь работает: синхронный lock в синхронном конструкторе
        with counter_lock:
            event_counter += 1
            self.id = str(event_counter)
        self.type = event_type
        self.data = data
        self.timestamp = datetime.utcnow().isoformat() + "Z"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "data": self.data,
            "timestamp": self.timestamp,
        }

    def to_json_bytes(self) -> bytes:
        return json.dumps(self.to_dict(), ensure_ascii=False).encode("utf-8")


async def init_kafka():
    global kafka_producer
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: v,
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
    )
    await kafka_producer.start()
    logger.info(f"🔗 Connected to Kafka brokers: {KAFKA_BROKERS}")


async def close_kafka():
    global kafka_producer
    if kafka_producer:
        await kafka_producer.stop()


async def publish_event(topic: str, event: Event):
    event_dict = event.to_dict()
    logger.info(f"📝 Processing {event.type} event #{event.id} with data: {event.data}")

    # Сохраняем в in-memory хранилище (только последние 50)
    async with store_lock:
        events_store.append(event_dict)
        if len(events_store) > 50:
            del events_store[0]

    # Отправляем в Kafka
    try:
        await kafka_producer.send_and_wait(
            topic=topic,
            key=event.id,
            value=event.to_json_bytes(),
        )
        logger.info(f"✅ Event #{event.id} successfully published to Kafka topic: {topic}")
    except Exception as e:
        logger.warning(f"⚠️ Failed to send to Kafka (fallback to local storage): {e}")
        # Событие всё равно сохранено локально

    # Имитация обработки события
    await process_event(event)


async def process_event(event: Event):
    logger.info(f"⚙️ Processing event #{event.id} of type '{event.type}'")

    data = event.data
    if event.type == "user_event":
        logger.info(f"👤 USER EVENT PROCESSED: {data}")
        if "user_id" in data:
            logger.info(f"   - User ID: {data['user_id']}")
        if "action" in data:
            logger.info(f"   - Action: {data['action']}")

    elif event.type == "movie_event":
        logger.info(f"🎬 MOVIE EVENT PROCESSED: {data}")
        if "movie_id" in data:
            logger.info(f"   - Movie ID: {data['movie_id']}")
        if "action" in data:
            logger.info(f"   - Action: {data['action']}")

    elif event.type == "payment_event":
        logger.info(f"💳 PAYMENT EVENT PROCESSED: {data}")
        if "amount" in data:
            logger.info(f"   - Amount: {data['amount']}")
        if "user_id" in data:
            logger.info(f"   - User ID: {data['user_id']}")

    else:
        logger.info(f"📋 GENERAL EVENT PROCESSED: {data}")

    logger.info(f"✅ Event #{event.id} processing completed at {datetime.utcnow().strftime('%H:%M:%S')}")


def get_events(event_type: Optional[str] = None) -> List[Dict[str, Any]]:
    if not event_type:
        return events_store.copy()
    return [e for e in events_store if e["type"] == event_type]


# === Роуты ===


@app.on_event("startup")
async def startup_event():
    await init_kafka()
    await initialize_demo_events()
    logger.info(f"🚀 Starting Events Service MVP with Kafka integration on port {PORT}")
    logger.info(f"📡 Kafka brokers: {', '.join(KAFKA_BROKERS)}")
    logger.info("🌐 Available endpoints:")
    logger.info("   POST /api/events/user    - Create user event")
    logger.info("   POST /api/events/movie   - Create movie event")
    logger.info("   POST /api/events/payment - Create payment event")
    logger.info("   GET  /api/events         - Get all events")
    logger.info("   GET  /api/events/health  - Health check")


@app.on_event("shutdown")
async def shutdown_event():
    await close_kafka()


@app.get("/health")
@app.get("/api/events/health")
async def health_check():
    events_count = len(events_store)
    response = {
        "status": True,
        "service": "events-service-mvp",
        "kafka_brokers": KAFKA_BROKERS,
        "events_count": events_count,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "endpoints": [
            "POST /api/events/user",
            "POST /api/events/movie",
            "POST /api/events/payment",
            "GET /api/events",
        ],
    }
    logger.info(f"🏥 Health check requested - service healthy with {events_count} events")
    return response


@app.get("/api/events")
async def list_events(request: Request):
    event_type = request.query_params.get("type")
    events = get_events(event_type)
    logger.info(f"📋 Events list requested (type: {event_type}) - returning {len(events)} events")
    return {"events": events, "count": len(events), "filter": event_type}


@app.post("/api/events/user")
async def handle_user_event(request: Request):
    return await handle_specific_event(request, "user_event", "user-events", "👤 USER")


@app.post("/api/events/movie")
async def handle_movie_event(request: Request):
    return await handle_specific_event(request, "movie_event", "movie-events", "🎬 MOVIE")


@app.post("/api/events/payment")
async def handle_payment_event(request: Request):
    return await handle_specific_event(request, "payment_event", "payment-events", "💳 PAYMENT")


@app.post("/api/events/publish")
async def handle_publish_event(request: Request):
    try:
        body = await request.json()
    except Exception:
        logger.error("❌ Generic event: Invalid JSON received")
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    event_type = body.get("type", "general")
    data = body.get("data", {})
    topic = f"{event_type}-events" if event_type else "general-events"

    logger.info(f"📥 Generic event creation requested: type={event_type}, data={data}")

    event = Event(event_type=event_type, data=data)
    await publish_event(topic, event)

    response = {
        "status": "success",
        "event_id": event.id,
        "topic": topic,
        "message": "Event created and processed successfully",
    }
    logger.info(f"✅ Generic event #{event.id} successfully created and published to {topic}")
    return JSONResponse(response, status_code=status.HTTP_201_CREATED)


async def handle_specific_event(
    request: Request, event_type: str, topic: str, log_prefix: str
):
    try:
        data = await request.json()
    except Exception:
        logger.error(f"❌ {log_prefix} event: Invalid JSON received")
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    logger.info(f"📥 {log_prefix} event creation requested with data: {data}")

    event = Event(event_type=event_type, data=data)
    await publish_event(topic, event)

    response = {
        "status": "success",
        "event_id": event.id,
        "topic": topic,
        "type": event_type,
        "message": f"{log_prefix} event created and processed successfully",
    }
    logger.info(f"✅ {log_prefix} event #{event.id} successfully created and published to {topic}")
    return JSONResponse(response, status_code=status.HTTP_201_CREATED)


async def initialize_demo_events():
    logger.info("🔄 Initializing demo events...")

    # Demo user event
    user_event = Event(
        event_type="user_event",
        data={"user_id": 1, "action": "login", "email": "demo@example.com"},
    )
    await publish_event("user-events", user_event)

    # Demo movie event
    movie_event = Event(
        event_type="movie_event",
        data={"movie_id": 1, "action": "viewed", "user_id": 1, "duration": 120},
    )
    await publish_event("movie-events", movie_event)


# === Запуск сервера ===
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)