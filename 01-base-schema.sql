CREATE DATABASE minas_data_lab;

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
