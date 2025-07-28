import uuid
import random
from faker import Faker
import psycopg2
from datetime import date
from typing import Optional

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
    client_ids = get_client_ids(cursor)

    if not client_ids:
        raise ValueError("Нет клиентов в таблице clients.clients.")

    acc_id = str(uuid.uuid4())
    acc_number = generate_unique_account_number()
    acc_type = random.choice(account_types)
    client_id = str(random.choice(client_ids))
    balance = round(random.uniform(0, 500000), 2)
    currency = 643
    open_date = fake.date_between(start_date='-5y', end_date='today')
    status = random.choice(account_statuses)
    close_date: Optional[date] = None
    if status == 'close':
        close_date = fake.date_between(start_date=open_date, end_date='today')

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

def generate_payload():
    conn = psycopg2.connect(**DB_SETTINGS)
    cursor = conn.cursor()

    accounts = generate_account_payload(cursor)
    account_id = accounts["id"]

    transactions = generate_transaction_payload(account_id, cursor)
    limits = generate_limit_payload(account_id)

    card_ids = get_card_ids(cursor)
    linked_cards = None
    if card_ids:
        card_id = random.choice(card_ids)
        linked_cards = generate_linked_card_payload(account_id, card_id)

    constant_info = generate_constant_info_payload()

    cursor.close()
    conn.close()

    return {
        "accounts": accounts,
        "transactions": transactions,
        "limits": limits,
        "linked_cards": linked_cards or {},
        "constant_information": constant_info
    }

if __name__ == "__main__":
    payload = generate_unified_payload()
    print(payload)
