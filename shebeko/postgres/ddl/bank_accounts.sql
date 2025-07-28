CREATE TYPE bank_accounts.accounts_type_enum AS ENUM (
  'credit',
  'deposit',
  'current'
);
ALTER TYPE bank_accounts.accounts_type_enum ADD VALUE 'system';

CREATE TYPE bank_accounts.accounts_status_enum AS ENUM (
  'active',
  'blocked',
  'close'
);
CREATE TYPE bank_accounts.limits_type_enum AS ENUM (
  'daily',
  'monthly'
);
CREATE TYPE bank_accounts.transactions_type_enum AS ENUM (
  'payment',
  'credit_payment',
  'transfer',
  'deposit',
  'refund',
  'withdrawal',
  'fee'
);
CREATE TABLE bank_accounts.accounts(
    id UUID NOT NULL,
    number CHAR(20) NOT NULL,
    type bank_accounts.accounts_type_enum NOT NULL,
    client_id UUID NOT NULL,
    balance DECIMAL(19, 2) NOT NULL DEFAULT '0.00',
    currency SMALLINT NOT NULL DEFAULT '643',
    open_date DATE NOT NULL,
    close_date DATE NULL,
    status bank_accounts.accounts_status_enum NOT NULL DEFAULT 'active'
);

ALTER TABLE bank_accounts.accounts
ADD CONSTRAINT bank_accounts_clients_id_foreign
FOREIGN KEY (client_id)
REFERENCES clients.clients(id);

ALTER TABLE
    bank_accounts.accounts ADD PRIMARY KEY(id);
ALTER TABLE
    bank_accounts.accounts ADD CONSTRAINT accounts_number_unique UNIQUE(number);
COMMENT
ON COLUMN
    bank_accounts.accounts.currency IS 'Валюта пока только рубли, поэтому код везде 643';
CREATE TABLE bank_accounts.limits(
    id UUID NOT NULL,
    account_id UUID NOT NULL,
    type bank_accounts.limits_type_enum NOT NULL,
    amount DECIMAL(19, 2) NOT NULL,
    start_date DATE NOT NULL,
    stop_date DATE NOT NULL
);
ALTER TABLE
    bank_accounts.limits ADD PRIMARY KEY(id);
CREATE TABLE bank_accounts.transactions(
    id UUID NOT NULL,
    account_id UUID NOT NULL,
    date_time TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    amount DECIMAL(19, 2) NOT NULL,
    type bank_accounts.transactions_type_enum NOT NULL,
    description VARCHAR(255) NULL
);
ALTER TABLE
    bank_accounts.transactions ADD PRIMARY KEY(id);
CREATE TABLE bank_accounts.linked_cards(
    id UUID NOT NULL,
    account_id UUID NOT NULL,
    card_id UUID NOT NULL,
    linked_date DATE NOT NULL,
    unlinked_date DATE NULL,
    is_main BOOLEAN NOT NULL DEFAULT '0'
);

ALTER TABLE bank_accounts.linked_cards
ADD CONSTRAINT bank_accounts_cards_id_foreign
FOREIGN KEY (card_id)
REFERENCES bank_cards.cards(id);

ALTER TABLE
    bank_accounts.linked_cards ADD PRIMARY KEY(id);
COMMENT
ON COLUMN
    bank_accounts.linked_cards. is_main IS 'true, false';
CREATE TABLE bank_accounts.constant_information(
    id UUID NOT NULL,
    bic CHAR(9) NOT NULL,
    inn CHAR(12) NOT NULL,
    kpp CHAR(9) NOT NULL,
    correspondent_account CHAR(20) NOT NULL,
    bank_name VARCHAR(255) NOT NULL
);
ALTER TABLE
    bank_accounts.constant_information ADD PRIMARY KEY(id);
ALTER TABLE
    bank_accounts.linked_cards ADD CONSTRAINT linked_cards_account_id_foreign FOREIGN KEY(account_id) REFERENCES bank_accounts.accounts(id);
ALTER TABLE
    bank_accounts.limits ADD CONSTRAINT limits_account_id_foreign FOREIGN KEY(account_id) REFERENCES bank_accounts.accounts(id);
ALTER TABLE
    bank_accounts.transactions ADD CONSTRAINT transactions_account_id_foreign FOREIGN KEY(account_id) REFERENCES bank_accounts.accounts(id);

ALTER TABLE bank_accounts.accounts
DROP CONSTRAINT bank_accounts_clients_id_foreign,
ADD CONSTRAINT bank_accounts_clients_id_foreign
FOREIGN KEY (client_id)
REFERENCES clients.clients(id)
ON DELETE CASCADE;

ALTER TABLE bank_accounts.linked_cards
DROP CONSTRAINT bank_accounts_cards_id_foreign,
ADD CONSTRAINT bank_accounts_cards_id_foreign
FOREIGN KEY (card_id)
REFERENCES bank_cards.cards(id)
ON DELETE CASCADE;

ALTER TABLE bank_accounts.linked_cards
DROP CONSTRAINT linked_cards_account_id_foreign,
ADD CONSTRAINT linked_cards_account_id_foreign
FOREIGN KEY (account_id)
REFERENCES bank_accounts.accounts(id)
ON DELETE CASCADE;

ALTER TABLE bank_accounts.limits
DROP CONSTRAINT limits_account_id_foreign,
ADD CONSTRAINT limits_account_id_foreign
FOREIGN KEY (account_id)
REFERENCES bank_accounts.accounts(id)
ON DELETE CASCADE;

ALTER TABLE bank_accounts.transactions
DROP CONSTRAINT transactions_account_id_foreign,
ADD CONSTRAINT transactions_account_id_foreign
FOREIGN KEY (account_id)
REFERENCES bank_accounts.accounts(id)
ON DELETE CASCADE;