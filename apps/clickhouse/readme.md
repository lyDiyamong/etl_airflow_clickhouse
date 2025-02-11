# Clickhouse Service - Sala's Data Warehouse

Clickhouse Service is a container which is served as a global data warehouse for Sala's eco-system.
At Sala, we design our platform in micro-service architecture and there are more than 20 services up and running. Data is stored accros the platform, so collecting and transforming them are needed.

Here are some uses of Clickhouse Service:

- Populate data from ETL process
- Data source which work with "Report Service" to generate different kind of report (Dynamic Report / Custom Report / Statistic Report)
- Scheduling for data clean up + data transforming

### Notes

1. Migration files are kept inside **_apps/clickhouse/migrations/yyyy_mm_dd_hh_mm_migration-name.sql_**
2. Migration file can be run only once, and it is marked in **_/var/lib/clickhouse/applied_migrations.txt_**
3. Migrations files are mounted to /migrations
4. custom_entrypoint.sh is used to trigger migrations as well as to create database "clickhouse" if it is not existed. Else we need to manual create it

### Some commands to work with

We can interact with clickhouse's data source through the following commands:

1. Access to clickhouse CLI
   ```sh
    docker exec -it clickhouse-server clickhouse-client
   ```
2. Database queries

- List all databases
  ```sql
  SHOW DATABASES;
  ```
- List all tables in "clickhouse" database
  ```sql
  SHOW TABLES IN clickhouse;
  ```
- List all columns in "student" table of "clickhouse" database
  ```sql
  DESCRIBE TABLE clickhouse.student;
  ```
- Drop database clickhouse
  ```sql
  DROP DATABASE clickhouse;
  ```

### How to rerun all the migrations

1. Access to clickhouse CLI
   ```sh
    docker exec -it clickhouse-server clickhouse-client
   ```
2. Drop database clickhouse
   ```sh
    DROP DATABASE clickhouse;
   ```
3. Remove applied migrations from **_/var/lib/clickhouse/applied_migrations.txt_**
   ```sh
    docker compose exec -it clickhouse-server bash
    cd /var/lib/clickhouse/
    rm applied_migrations.txt
   ```
4. Restart clickhouse-server
   ```sh
    docker compose restart clickhouse-server
   ```
