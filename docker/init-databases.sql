-- Скрипт автоматически выполняется при первом запуске контейнера PostgreSQL.
-- База telemetry_analyzer создаётся через POSTGRES_DB в docker-compose.yml.
-- Здесь создаются дополнительные базы данных для бизнес-сервисов.

CREATE DATABASE product_db;
CREATE DATABASE inventory_db;
CREATE DATABASE order_db;
