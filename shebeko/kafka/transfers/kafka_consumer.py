import json
from kafka import KafkaConsumer
import psycopg2

KAFKA_BROKER = '***'
TOPIC_LIST = [
    '****'
]


DB_SETTINGS = {
    'dbname': 'bankdb',
    'user': '',
    'password': 'pass',
    'host': '',
    'port': ''
}

def log(msg, level='INFO'):
    print(f"[{level}] {msg}")


def connect_db(config):
    try:
        connection = psycopg2.connect(**config)
        connection.cursor().execute("SET search_path TO bank_accounts;")
        log("Подключение к БД успешно.")
        return connection
    except Exception as err:
        log(f"Не удалось подключиться к БД: {err}", level="ERROR")
        raise


def setup_consumer(topics, broker):
    return KafkaConsumer(
        *topics,
        bootstrap_servers=broker,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='bank_account_group_3'
    )


def save_record(cursor, table_name, record):
    if not record:
        log(f"Пропущена вставка в '{table_name}': пустой payload", level="WARNING")
        return

    cols = ', '.join(record.keys())
    placeholders = ', '.join(['%s'] * len(record))
    values = list(record.values())

    sql = f"""
    INSERT INTO bank_accounts.{table_name} ({cols})
    VALUES ({placeholders})
    ON CONFLICT (id) DO NOTHING
    """

    try:
        cursor.execute(sql, values)
        log(f"Добавлена запись в '{table_name}': {record.get('id') or record.get('account_id')}")
    except Exception as ex:
        log(f"Ошибка при вставке в '{table_name}': {ex}", level="ERROR")
        raise


def main():
    log("Запуск consumer...")
    consumer = setup_consumer(TOPIC_LIST, KAFKA_BROKER)
    db = connect_db(DB_SETTINGS)
    cur = db.cursor()

    prefix = "bank_accounts."

    for msg in consumer:

        topic = msg.topic
        payload = msg.value

        if topic.startswith(prefix):
            table = topic.replace(prefix, "")
            try:
                save_record(cur, table, payload)
                db.commit()
            except:
                db.rollback()


if __name__ == "__main__":
    main()
