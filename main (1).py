"""Movie Service: HTTP API приёма событий + генератор синтетических событий."""
import logging
import os
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

import generator
from kafka_io import EventPublisher, PublishError, SchemaValidationError
from models import MovieEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("movie-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    publisher = EventPublisher()
    app.state.publisher = publisher
    stop = threading.Event()
    if os.getenv("GENERATOR_ENABLED", "true").lower() == "true":
        threading.Thread(target=generator.run, args=(publisher, stop), daemon=True).start()
    yield
    stop.set()
    publisher.close()


app = FastAPI(title="Movie Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/events", status_code=201)
def post_event(event: MovieEvent):
    """Принимает событие (JSON). Ошибка валидации -> 422, успех -> event_id."""
    try:
        info = app.state.publisher.publish(event)
    except SchemaValidationError as e:
        raise HTTPException(status_code=422, detail=f"schema validation failed: {e}")
    except PublishError as e:
        raise HTTPException(status_code=503, detail=str(e))
    return {"event_id": str(event.event_id), **info}
