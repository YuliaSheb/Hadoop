CREATE TYPE cash_transactions.client_operations_type_enum AS ENUM (
  'deposit',
  'withdrawal'
);
 
CREATE TYPE cash_transactions.client_operations_status_enum AS ENUM (
  'completed',
  'in_progress',
  'error'
);
 
CREATE TYPE cash_transactions.client_operations_perfomed_at_enum AS ENUM (
  'atm',
  'office'
);
 
CREATE TYPE cash_transactions.office_operations_type_enum AS ENUM (
  'client_operation',
  'encashment',
  'payment'
);
 
CREATE TYPE cash_transactions.atm_operations_type_enum AS ENUM (
  'client_operation',
  'encashment',
  'payment'
);

CREATE TABLE cash_transactions.client_operations(
    id UUID NOT NULL,
    type cash_transactions.client_operations_type_enum NOT NULL,
    amount DECIMAL(19, 2) NOT NULL,
    currency CHAR(3) NOT NULL DEFAULT '643',
    date_time TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    status cash_transactions.client_operations_status_enum NOT NULL,
    account_id UUID NOT NULL,
    client_id UUID NOT NULL,
    perfomed_at cash_transactions.client_operations_perfomed_at_enum NOT NULL
);

ALTER TABLE cash_transactions.client_operations
DROP CONSTRAINT client_operation_bank_accounts_id_foreign,
ADD CONSTRAINT client_operation_bank_accounts_id_foreign
FOREIGN KEY (account_id)
REFERENCES bank_accounts.accounts(id)
ON DELETE CASCADE;

ALTER TABLE cash_transactions.client_operations
DROP CONSTRAINT client_operation_clients_id_foreign,
ADD CONSTRAINT client_operation_clients_id_foreign
FOREIGN KEY (client_id)
REFERENCES clients.clients(id)
ON DELETE CASCADE;

ALTER TABLE cash_transactions.client_operations ADD PRIMARY KEY("id");
	
CREATE TABLE cash_transactions.atm_operations(
    "id" UUID NOT NULL,
    "atm_id" UUID NOT NULL,
    "balance" DECIMAL(19, 2) NOT NULL,
    "type" cash_transactions.atm_operations_type_enum NOT NULL,
    "date_time" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    "client_operaton_id" UUID NULL,
    "encashment_id" UUID NULL,
    "payment_id" UUID NULL,
    "amount" DECIMAL(19, 2) NOT NULL
);

ALTER TABLE cash_transactions.atm_operations
--DROP CONSTRAINT atm_operation_atm_id_foreign,
ADD CONSTRAINT atm_operation_atm_id_foreign
FOREIGN KEY (atm_id)
REFERENCES cash_machines.atms(id)
ON DELETE CASCADE;

ALTER TABLE cash_transactions.atm_operations
DROP CONSTRAINT atm_operation_encashment_id_foreign,
ADD CONSTRAINT atm_operation_encashment_id_foreign
FOREIGN KEY (encashment_id)
REFERENCES cash_machines.atm_maintenance(id)
ON DELETE CASCADE;

ALTER TABLE cash_transactions.atm_operations
DROP CONSTRAINT atm_operation_payments_id_foreign,
ADD CONSTRAINT atm_operation_payments_id_foreign
FOREIGN KEY (payment_id)
REFERENCES payments.payments(id)
ON DELETE CASCADE;


ALTER TABLE
    cash_transactions."atm_operations" ADD PRIMARY KEY("id");
CREATE TABLE cash_transactions."office_operations"(
    "id" UUID NOT NULL,
    "office_id" UUID NOT NULL,
    "balance" DECIMAL(19, 2) NOT NULL,
    "type" cash_transactions.office_operations_type_enum NOT NULL,
    "date_time" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    "client_operation_id" UUID NULL,
    "encashment_id" UUID NULL,
    "payment_id" UUID NULL,
    "amount" DECIMAL(19, 2) NOT NULL
);

ALTER TABLE cash_transactions.office_operations
DROP CONSTRAINT office_operation_payments_id_foreign,
ADD CONSTRAINT office_operation_payments_id_foreign
FOREIGN KEY (payment_id)
REFERENCES payments.payments(id)
ON DELETE CASCADE;

ALTER TABLE cash_transactions.office_operations
DROP CONSTRAINT office_operation_banks_office_id_foreign,
ADD CONSTRAINT office_operation_banks_office_id_foreign
FOREIGN KEY (office_id)
REFERENCES bank_branches.bank_office(id)
ON DELETE CASCADE;

ALTER TABLE cash_transactions.office_operations
DROP CONSTRAINT office_operation_banks_encashment_id_foreign,
ADD CONSTRAINT office_operation_banks_encashment_id_foreign
FOREIGN KEY (encashment_id)
REFERENCES bank_branches.encashment(id)
ON DELETE CASCADE;

ALTER TABLE
    cash_transactions."office_operations" ADD PRIMARY KEY("id");
ALTER TABLE
    cash_transactions."office_operations" ADD CONSTRAINT "office_operations_client_operation_id_foreign" FOREIGN KEY("client_operation_id") REFERENCES cash_transactions."client_operations"("id");
ALTER TABLE
    cash_transactions."atm_operations" ADD CONSTRAINT "atm_operations_client_operaton_id_foreign" FOREIGN KEY("client_operaton_id") REFERENCES cash_transactions."client_operations"("id");

SELECT * FROM cash_transactions.client_operations