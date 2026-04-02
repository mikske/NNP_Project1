from __future__ import annotations

import os
from typing import Optional

import aio_pika
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


# =========================
# Общие модели/настройки
# =========================

class PublishRequest(BaseModel):
    """Модель входных данных для producer-сервиса."""
    message: str


# =========================
# Шаблон 1: Producer (FastAPI)
# =========================

producer_app = FastAPI(title="Producer API (Template)")

#загружаем конфигурацию приложения из переменных окружения
def load_config() -> dict:
    load_dotenv()

    rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672//")
    queue_name = os.getenv("QUEUE_NAME", "my_queue")

    if not queue_name or not queue_name.strip():
        raise ValueError("QUEUE_NAME не должен быть пустым")

    return {
        "rabbitmq_url": rabbitmq_url,
        "queue_name": queue_name.strip(),
    }

#устанавливаем асинхронное подключение
async def connect_rabbitmq(rabbitmq_url: str):
    connection = await aio_pika.connect_robust(rabbitmq_url)
    return connection

#создаем канал и объявляем очередь, если она еще не существует
async def ensure_queue(connection, queue_name: str):
    channel = await connection.channel()
    queue = await channel.declare_queue(queue_name, durable=True)
    return channel, queue

#отправляем сообщение в очередь RabbitMQ
async def publish_message(channel, queue_name: str, payload: str) -> None:
    message = aio_pika.Message(
        body=payload.encode("utf-8"),
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )

    await channel.default_exchange.publish(
        message,
        routing_key=queue_name,
    )

#подготавливаем подключение к RabbitMQ при старте FastAPI-приложения
@producer_app.on_event("startup")
async def on_startup() -> None:
    config = load_config()

    connection = await connect_rabbitmq(config["rabbitmq_url"])
    channel, _queue = await ensure_queue(connection, config["queue_name"])

    producer_app.state.connection = connection
    producer_app.state.channel = channel
    producer_app.state.queue_name = config["queue_name"]
    producer_app.state.config = config

#закрываем подключение к RabbitMQ при остановке приложения
@producer_app.on_event("shutdown")
async def on_shutdown() -> None:
    connection = getattr(producer_app.state, "connection", None)
    if connection is not None:
        await connection.close()

#реализуем HTTP-эндпоинт
@producer_app.post("/publish")
async def publish_endpoint(req: PublishRequest):
    if not req.message or not req.message.strip():
        raise HTTPException(status_code=400, detail="Пустое сообщение отправлять нельзя")

    channel = getattr(producer_app.state, "channel", None)
    queue_name = getattr(producer_app.state, "queue_name", None)

    if channel is None or queue_name is None:
        raise HTTPException(status_code=500, detail="RabbitMQ не инициализирован")

    await publish_message(channel, queue_name, req.message.strip())

    return {
        "status": "ok",
        "sent": req.message.strip(),
    }