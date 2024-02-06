CREATE DATABASE gov_transparency_hub;
CREATE DATABASE dagster;

\c gov_transparency_hub;

CREATE TABLE "revenue" (
    "id" SERIAL PRIMARY KEY,
    "date" date NOT NULL,
    "draft_type" TEXT NOT NULL,
    "revenue" TEXT NOT NULL,
    "source" TEXT NOT NULL,
    "resource_source" TEXT NOT NULL,
    "co_tce" TEXT NOT NULL,
    "co_aux" TEXT NOT NULL,
    "historic" TEXT,
    "value" DECIMAL NOT NULL,
    "city" VARCHAR(40) NOT NULL
);


CREATE TABLE "expense" (
    "number" TEXT NOT NULL,
    "year" INT NOT NULL,
    "type" TEXT NOT NULL,
    "effort_date" TEXT,
    "settlement_date" TEXT,
    "payment_date" TEXT,
    "unit" TEXT NOT NULL,
    "function" TEXT NOT NULL,
    "subfunction" TEXT NOT NULL,
    "program" TEXT NOT NULL,
    "activity" TEXT NOT NULL,
    "economic_category" TEXT NOT NULL,
    "resource_source" TEXT NOT NULL,
    "co_tce" TEXT NOT NULL,
    "co_aux" TEXT NOT NULL,
    "value" DECIMAL NOT NULL,
    "creditor" TEXT NOT NULL,
    "cpf_cnpj" VARCHAR(18) NOT NULL,
    "history" TEXT NOT NULL,
    "city" TEXT NOT NULL,
    PRIMARY KEY ("number", "year", "city")
);
