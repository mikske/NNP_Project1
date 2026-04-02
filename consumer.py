from __future__ import annotations

import os
import sys
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
    ru_queue_name = os.getenv("RU_QUEUE_NAME", "ru_queue")

    if not queue_name or not queue_name.strip():
        raise ValueError("QUEUE_NAME не должен быть пустым")

    return {
        "rabbitmq_url": rabbitmq_url,
        "queue_name": queue_name.strip(),
        "ru_queue_name": ru_queue_name.strip(),
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
async def consume(selected_queue_name: str) -> None:
    config = load_config()
    connection = await connect_rabbitmq(config["rabbitmq_url"])

    try:
        _channel, queue = await ensure_queue(connection, selected_queue_name)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                await process_message(message)
    finally:
        await connection.close()


if __name__ == "__main__":
    import asyncio

    config = load_config()

    selected_queue = config["queue_name"]

    #при запуске ru слушаем очередь ru
    if len(sys.argv) > 1 and sys.argv[1] == "ru":
        selected_queue = config["ru_queue_name"]

    asyncio.run(consume(selected_queue))