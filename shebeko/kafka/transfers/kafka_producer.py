import time
import json
from kafka import KafkaProducer
from generator import generate_payload

KAFKA_SERVERS = '****'

def init_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda data: json.dumps(data).encode('utf-8')
    )

TOPIC_ACCOUNTS = 'bank_accounts.accounts'
TOPIC_LIMITS = 'bank_accounts.limits'
TOPIC_TRANSACTIONS = 'bank_accounts.transactions'
TOPIC_LINKED_CARDS = 'bank_accounts.linked_cards'
TOPIC_CONSTANT_INFO = 'bank_accounts.constant_information'


def push_to_topic(producer, topic, payload, info=""):
    try:
        producer.send(topic, value=payload)
        producer.flush()
        if info:
            print(f"[INFO] {info}")
    except Exception as ex:
        print(f"[ERROR] Failed to send message to {topic}: {ex}")


def handle_account(producer, account_data):
    push_to_topic(
        producer,
        TOPIC_ACCOUNTS,
        account_data,
        f"Отправлен счёт: {account_data.get('id')}"
    )


def handle_limit(producer, limit_data):
    push_to_topic(
        producer,
        TOPIC_LIMITS,
        limit_data,
        f"Отправлен лимит: {limit_data.get('id') or limit_data.get('account_id')}"
    )


def handle_transaction(producer, txn_data):
    push_to_topic(
        producer,
        TOPIC_TRANSACTIONS,
        txn_data,
        f"Отправлена транзакция: {txn_data.get('id')}"
    )


def handle_card(producer, card_data):
    push_to_topic(
        producer,
        TOPIC_LINKED_CARDS,
        card_data,
        f"Отправлена карта: {card_data.get('id')}"
    )


def handle_constant_info(producer, info_data):
    push_to_topic(
        producer,
        TOPIC_CONSTANT_INFO,
        info_data,
        f"Отправлена постоянная информация для аккаунта: {info_data.get('id')}"
    )


def main_loop():
    producer = init_producer()
    print("[INFO] Kafka-продюсер инициализирован. Цикл генерации запущен.")

    while True:
        try:
            payload = generate_payload()

            handle_account(producer, payload['accounts'])
            time.sleep(2)

            handle_limit(producer, payload['limits'])
            time.sleep(1)

            handle_transaction(producer, payload['transactions'])
            time.sleep(1)

            handle_card(producer, payload['linked_cards'])
            time.sleep(1)

            handle_constant_info(producer, payload['constant_information'])

        except Exception as e:
            print(f"[ERROR] Ошибка при отправке данных: {e}")

        print("[INFO] Пауза перед следующей итерацией...\n")
        time.sleep(6)


calls_per_day = 1000
seconds_per_day = 24 * 60 * 60 
interval = seconds_per_day / calls_per_day

print(f"Интервал между вызовами: {interval:.2f} сек")

for i in range(calls_per_day):
    main_loop()
    time.sleep(interval)