CREATE CATALOG IF NOT EXISTS retail_demo;

CREATE SCHEMA IF NOT EXISTS retail_demo.bronze;
CREATE SCHEMA IF NOT EXISTS retail_demo.silver;
CREATE SCHEMA IF NOT EXISTS retail_demo.gold;

CREATE VOLUME IF NOT EXISTS retail_demo.bronze.raw_data;