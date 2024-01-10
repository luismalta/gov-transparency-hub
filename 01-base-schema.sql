CREATE DATABASE minas_data_lab;
CREATE DATABASE dagster;

\c minas_data_lab;

CREATE TABLE "revenue" (
    "date" DATE NOT NULL,
    "draft_type" VARCHAR(100) NOT NULL,
    "revenue" VARCHAR(100) NOT NULL,
    "source" VARCHAR(100) NOT NULL,
    "resource_source" VARCHAR(100) NOT NULL,
    "co_tce" VARCHAR(100) NOT NULL,
    "co_aux" VARCHAR(100) NOT NULL,
    "historic" VARCHAR(100) NOT NULL,
    "value" DECIMAL NOT NULL
);

CREATE TABLE "expense" (
    "effort" VARCHAR(100) NOT NULL,
    "creditor" VARCHAR(100) NOT NULL,
    "type" VARCHAR(100) NOT NULL,
    "bidding_process" VARCHAR(100),
    "effort_date" DATE,
    "settlement_date" DATE,
    "payment_date" DATE,
    "effort_value" VARCHAR(100),
    "settlement_value" DECIMAL,
    "paid_value" DECIMAL
);
