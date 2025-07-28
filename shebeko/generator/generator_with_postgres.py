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

def get_card_ids(cursor):
    cursor.execute("SELECT id FROM bank_cards.cards")
    return [row[0] for row in cursor.fetchall()]

def get_account_ids(cursor):
    cursor.execute("SELECT id FROM bank_accounts.accounts")
    return [row[0] for row in cursor.fetchall()]

used_numbers = set()

def generate_unique_account_number():
    while True:
        number = ''.join([str(random.randint(0, 9)) for _ in range(20)])
        if number not in used_numbers:
            used_numbers.add(number)
            return number

def generate_account_payload(cursor) -> dict:
    account_types = get_enum_values(cursor, 'bank_accounts.accounts_type_enum')
    account_statuses = get_enum_values(cursor, 'bank_accounts.accounts_status_enum')

    acc_id = str(uuid.uuid4())
    acc_number = generate_unique_account_number()
    acc_type = random.choice(account_types)
    currency = 643
    open_date = fake.date_between(start_date='-5y', end_date='today')
    status = 'active'
    close_date: Optional[date] = None
    if status == 'close':
        close_date = fake.date_between(start_date=open_date, end_date='today')

    if acc_type == 'system':
        cursor.execute("""
            SELECT cl.id
            FROM clients.clients cl
            JOIN clients.companies c ON c.client_id = cl.id
            WHERE cl.client_type = 'company'
              AND LOWER(c.company_name) LIKE '%банк%'
        """)
    else:
        cursor.execute("SELECT id FROM clients.clients")

    client_ids = [row[0] for row in cursor.fetchall()]

    if not client_ids:
        raise ValueError("Нет подходящих клиентов в базе для типа счёта: " + acc_type)

    client_id = str(random.choice(client_ids))
    balance = round(random.uniform(0, 500000), 2)

    return {
        "id": acc_id,
        "number": acc_number,
        "type": acc_type,
        "client_id": client_id,
        "balance": balance,
        "currency": currency,
        "open_date": open_date.isoformat(),
        "close_date": close_date.isoformat() if close_date else None,
        "status": status
    }


def generate_transaction_payload(account_id: str, cursor) -> dict:
    transaction_types = get_enum_values(cursor, 'bank_accounts.transactions_type_enum')
    return {
        "id": str(uuid.uuid4()),
        "account_id": account_id,
        "date_time": fake.date_time_between(start_date='-2y', end_date='now').isoformat(),
        "amount": round(random.uniform(100, 10000), 2),
        "type": random.choice(transaction_types),
        "description": fake.sentence(nb_words=6)
    }

def generate_limit_payload(account_id: str) -> dict:
    limit_type = random.choice(['daily', 'monthly'])
    start_date = fake.date_between(start_date='-2y', end_date='today')
    stop_date = fake.date_between(start_date=start_date, end_date='+1y')
    return {
        "id": str(uuid.uuid4()),
        "account_id": account_id,
        "type": limit_type,
        "amount": round(random.uniform(1000, 100000), 2),
        "start_date": start_date.isoformat(),
        "stop_date": stop_date.isoformat()
    }

def generate_linked_card_payload(account_id: str, card_id: str) -> dict:
    linked_date = fake.date_between(start_date='-2y', end_date='today')
    unlinked_date = None
    if random.choice([True, False]):
        unlinked_date = fake.date_between(start_date=linked_date, end_date='today')
    return {
        "id": str(uuid.uuid4()),
        "account_id": account_id,
        "card_id": card_id,
        "linked_date": linked_date.isoformat(),
        "unlinked_date": unlinked_date.isoformat() if unlinked_date else None,
        "is_main": random.choice([True, False])
    }

def generate_constant_info_payload() -> dict:
    bic = ''.join([str(random.randint(0, 9)) for _ in range(9)])
    return {
        "id": str(uuid.uuid4()),
        "bic": bic,
        "inn": ''.join([str(random.randint(0, 9)) for _ in range(12)]),
        "kpp": ''.join([str(random.randint(0, 9)) for _ in range(9)]),
        "correspondent_account": "30101" + ''.join([str(random.randint(0, 9)) for _ in range(12)]) + bic[-3:],
        "bank_name": fake.company()
    }

def insert_account(cursor, acc):
    cursor.execute("""
        INSERT INTO bank_accounts.accounts 
        (id, number, type, client_id, balance, currency, open_date, close_date, status)
        VALUES (%(id)s, %(number)s, %(type)s, %(client_id)s, %(balance)s, %(currency)s, %(open_date)s, %(close_date)s, %(status)s)
    """, acc)

def insert_transaction(cursor, txn):
    cursor.execute("""
        INSERT INTO bank_accounts.transactions 
        (id, account_id, date_time, amount, type, description)
        VALUES (%(id)s, %(account_id)s, %(date_time)s, %(amount)s, %(type)s, %(description)s)
    """, txn)

def insert_limit(cursor, limit):
    cursor.execute("""
        INSERT INTO bank_accounts.limits 
        (id, account_id, type, amount, start_date, stop_date)
        VALUES (%(id)s, %(account_id)s, %(type)s, %(amount)s, %(start_date)s, %(stop_date)s)
    """, limit)

def insert_linked_card(cursor, link):
    cursor.execute("""
        INSERT INTO bank_accounts.linked_cards 
        (id, account_id, card_id, linked_date, unlinked_date, is_main)
        VALUES (%(id)s, %(account_id)s, %(card_id)s, %(linked_date)s, %(unlinked_date)s, %(is_main)s)
    """, link)

    """def insert_constant_info(cursor, const):
    cursor.execute(\"""
        INSERT INTO bank_accounts.constant_information 
        (id, bic, inn, kpp, correspondent_account, bank_name)
        VALUES (%(id)s, %(bic)s, %(inn)s, %(kpp)s, %(correspondent_account)s, %(bank_name)s)
    \""", const)"""

def generate_payload():
    conn = psycopg2.connect(**DB_SETTINGS)
    cursor = conn.cursor()

    account = generate_account_payload(cursor)
    insert_account(cursor, account)

    transaction = generate_transaction_payload(account["id"], cursor)
    insert_transaction(cursor, transaction)

    limit = generate_limit_payload(account["id"])
    insert_limit(cursor, limit)

    linked_card = {}
    card_ids = get_card_ids(cursor)
    account_ids = get_account_ids(cursor)
    if card_ids:
        card_id = random.choice(card_ids)
        account_id = random.choice(account_ids)
        linked_card = generate_linked_card_payload(account_id, card_id)
        insert_linked_card(cursor, linked_card)

    #const_info = generate_constant_info_payload()
    #insert_constant_info(cursor, const_info)

    conn.commit()
    cursor.close()
    conn.close()

    return {
        "accounts": account,
        "transactions": transaction,
        "limits": limit,
        "linked_cards": linked_card or {},
        #"constant_information": const_info
    }


@scheduler.scheduled_job('cron', minute='*', hour='9-20')
def job():
    print(f"[{datetime.now()}] Генерация данных...")
    generate_payload()

print("Запущен планировщик (каждую минуту с 9:00 до 21:00)")
scheduler.start()