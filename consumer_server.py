import asyncio

from confluent_kafka import Consumer

BROKER_URL = "PLAINTEXT://localhost:9092"

async def consume(topic_name):
    """Consumes data from a Kafka Topic"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])

    while True:
        messages = c.consume()
        for message in messages:
            if message is None:
                print('No message')
            elif message.error() is not None:
                print(f'Message error: {message.error()}')
            else:
                print(f'{message.value()}\n')
  
        await asyncio.sleep(0.01)


def main():
    try:
        asyncio.run(consume("police_calls"))
    except KeyboardInterrupt as e:
        print("shut down")
        
if __name__ == '__main__':
    main()