import uuid
import random
from faker import Faker
import psycopg2
from datetime import date
from typing import Optional
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime

scheduler = BlockingScheduler()

fake = Faker()

DB_SETTINGS = {
    'dbname': 'bankdb',
    'user': '',
    'password': 'pass',
    'host': '',
    'port': ''
}

def get_enum_values(cursor, enum_type_name: str):
    cursor.execute(f"SELECT unnest(enum_range(NULL::{enum_type_name}))")
    return [row[0] for row in cursor.fetchall()]

def get_client_ids(cursor):
    cursor.execute("SELECT id FROM clients.clients")
    return [row[0] for row in cursor.fetchall()]

def get_account_ids(cursor):
    cursor.execute("SELECT id FROM bank_accounts.accounts")
    return [row[0] for row in cursor.fetchall()]

def get_atm_ids(cursor):
    cursor.execute("SELECT id FROM cash_machines.atms")
    return [row[0] for row in cursor.fetchall()]

def get_encashment_ids(cursor):
    cursor.execute("SELECT id FROM cash_machines.atm_maintenance")
    return [row[0] for row in cursor.fetchall()]

def get_payment_ids(cursor):
    cursor.execute("SELECT id FROM payments.payments")
    return [row[0] for row in cursor.fetchall()]

def get_office_ids(cursor):
    cursor.execute("SELECT id FROM bank_branches.bank_office")
    return [row[0] for row in cursor.fetchall()]

def get_office_encashment_ids(cursor):
    cursor.execute("SELECT id FROM bank_branches.encashment")
    return [row[0] for row in cursor.fetchall()]

def get_office_clients_ids(cursor) -> list[dict]:
    cursor.execute("SELECT id,amount FROM cash_transactions.client_operations where perfomed_at='office'")
    return [{"id": str(row[0]), "amount": float(row[1])} for row in cursor.fetchall()]

def get_atm_clients_ids(cursor) -> list[dict]:
    cursor.execute("SELECT id, amount FROM cash_transactions.client_operations where perfomed_at = 'atm'")
    return [{"id": str(row[0]), "amount": float(row[1])} for row in cursor.fetchall()]

def generate_client_operation_payload(cursor) -> dict:
    client_operation_types = get_enum_values(cursor, 'cash_transactions.client_operations_type_enum')
    client_operation_statuses = get_enum_values(cursor, 'cash_transactions.client_operations_status_enum')
    account_ids = get_account_ids(cursor)
    client_ids = get_client_ids(cursor)
    client_operation_perfomed_at = get_enum_values(cursor, 'cash_transactions.client_operations_perfomed_at_enum')

    if not client_ids:
        raise ValueError("Нет клиентов в таблице clients.clients.")

    if not account_ids:
        raise ValueError("Нет счетов в таблице bank_accounts.accounts.")

    op_id = str(uuid.uuid4())
    op_type = random.choice(client_operation_types)
    amount = round(random.uniform(100, 10000), 2)
    currency = 643
    date_time = fake.date_time_between(start_date='-5y', end_date='now').isoformat()
    status = random.choice(client_operation_statuses)
    account_id = str(random.choice(account_ids))
    client_id = str(random.choice(client_ids))
    perfomed_at = random.choice(client_operation_perfomed_at)

    return {
        "id": op_id,
        "type": op_type,
        "amount": amount,
        "currency": currency,
        "date_time": date_time,
        "status": status,
        "account_id": account_id,
        "client_id": client_id,
        "perfomed_at":perfomed_at
    }

def generate_atm_operations_payload(cursor) -> dict:
    atm_operation_types = get_enum_values(cursor, 'cash_transactions.atm_operations_type_enum')
    atm_ids = get_atm_ids(cursor)
    atm_clients_ids = get_atm_clients_ids(cursor)
    encashment_ids = get_encashment_ids(cursor)
    payment_ids = get_payment_ids(cursor)
    if not atm_ids:
        raise ValueError("Нет банкоматов в таблице cash_mashines.atms.")
    if not encashment_ids:
        raise ValueError("Нет значений в таблице cash_mashines.atms.")
    if not payment_ids:
        raise ValueError("Нет платежей в таблице payments.payments.")
    if not atm_clients_ids:
        raise ValueError("Нет операций performed_at='atm' в client_operations")

    selected_operation = random.choice(atm_clients_ids)
    return {
        "id": str(uuid.uuid4()),
        "atm_id": str(random.choice(atm_ids)),
        "balance": round(random.uniform(0, 500000), 2),
        "type": random.choice(atm_operation_types),
        "date_time": fake.date_time_between(start_date='-4y', end_date='now').isoformat(),
        "client_operaton_id": selected_operation["id"],
        "encashment_id": str(random.choice(encashment_ids)),
        "payment_id": str(random.choice(payment_ids)),
        "amount": selected_operation["amount"]
    }

def generate_office_operations_payload(cursor) -> dict:
    office_operation_types = get_enum_values(cursor, 'cash_transactions.office_operations_type_enum')
    office_clients_ids = get_office_clients_ids(cursor)
    office_ids = get_office_ids(cursor)
    office_encashment_ids = get_office_encashment_ids(cursor)
    payment_ids = get_payment_ids(cursor)

    if not office_clients_ids:
        raise ValueError("Нет операций performed_at='office' в client_operations")

    selected_operation = random.choice(office_clients_ids)

    return {
        "id": str(uuid.uuid4()),
        "office_id": str(random.choice(office_ids)),
        "balance": round(random.uniform(0, 500000), 2),
        "type": random.choice(office_operation_types),
        "date_time": fake.date_time_between(start_date='-4y', end_date='now').isoformat(),
        "client_operation_id": selected_operation["id"],
        "encashment_id": str(random.choice(office_encashment_ids)),
        "payment_id": str(random.choice(payment_ids)),
        "amount": selected_operation["amount"]
    }


def insert_client_op(cursor, client_op):
    cursor.execute("""
        INSERT INTO cash_transactions.client_operations 
        (id, type, amount, currency, date_time, status, account_id, client_id, perfomed_at)
        VALUES (%(id)s, %(type)s, %(amount)s, %(currency)s, %(date_time)s, %(status)s, %(account_id)s, %(client_id)s, %(perfomed_at)s)
    """, client_op)

def insert_atm_op(cursor, atm):
    cursor.execute("""
        INSERT INTO cash_transactions.atm_operations
        (id, atm_id, balance, type, date_time, client_operaton_id, encashment_id, payment_id, amount)
        VALUES (%(id)s, %(atm_id)s, %(balance)s, %(type)s, %(date_time)s, %(client_operaton_id)s, %(encashment_id)s, %(payment_id)s, %(amount)s)
    """, atm)

def insert_office_op(cursor, office):
    cursor.execute("""
        INSERT INTO cash_transactions.office_operations
        (id, office_id, balance, type, date_time, client_operation_id, encashment_id, payment_id, amount)
        VALUES (%(id)s, %(office_id)s, %(balance)s, %(type)s, %(date_time)s, %(client_operation_id)s, %(encashment_id)s, %(payment_id)s, %(amount)s)
    """, office)


def generate_payload():
    conn = psycopg2.connect(**DB_SETTINGS)
    cursor = conn.cursor()

    client_operation = generate_client_operation_payload(cursor)
    insert_client_op(cursor, client_operation)

    atm_operation = generate_atm_operations_payload(cursor)
    insert_atm_op(cursor, atm_operation)

    office_operation = generate_office_operations_payload(cursor)
    insert_office_op(cursor, office_operation)

    conn.commit()
    cursor.close()
    conn.close()

    return {
        "client_operations": client_operation,
        "atm_operations": atm_operation,
        "office_operations": office_operation
    }


@scheduler.scheduled_job('cron', minute='*', hour='9-20')
def job():
    print(f"[{datetime.now()}] Генерация данных...")
    generate_payload()

print("Запущен планировщик (каждую минуту с 9:00 до 21:00)")
scheduler.start()