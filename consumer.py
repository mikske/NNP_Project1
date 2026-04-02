from __future__ import annotations

import os
from typing import Optional

import aio_pika
from dotenv import load_dotenv


# =========================
# Общие настройки
# =========================

#загружаем конфигурацию consumer-приложения из переменных окружения
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

#устанавливаем асинхронное подключеник к RabbitMQ
async def connect_rabbitmq(rabbitmq_url: str):
    connection = await aio_pika.connect_robust(rabbitmq_url)
    return connection

#создаем канал и объявляем очередь, если она еще не существует
async def ensure_queue(connection, queue_name: str):
    channel = await connection.channel()
    queue = await channel.declare_queue(queue_name, durable=True)
    return channel, queue

#обрабатываем входящее сообщение из очнереди
async def process_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
    async with message.process():
        payload = message.body.decode("utf-8")
        print(f"Received message: {payload}")

#запускаем consumer, который слушает очередь и обрабатывает сообщения
async def consume() -> None:
    config = load_config()
    connection = await connect_rabbitmq(config["rabbitmq_url"])

    try:
        _channel, queue = await ensure_queue(connection, config["queue_name"])

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                await process_message(message)
    finally:
        await connection.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(consume())