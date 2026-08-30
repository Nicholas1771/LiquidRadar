# LiquidRadar
Load real time BTCUSD trades from Coinbase with Flink and publish them to Kafka for Snowflake ingestion.
Flow: Coinbase API -> Flink -> Kafka -> Snowflake table

# Setup & Run
1. Create snowflake table in snowflake/snowflake_sql_setup.sql
2. Generate key and set the snowflake rsa key
3. Download Docker Desktop
4. In docker desktop terminal navigate to LiquidRadar/docker
5. Create .env from .env.example with your snowflake user, id, and key
6. ./gradlew clean build
7. In docker desktop terminal run: docker compose up -d
8. If previous step gets error, try to delete the project in docker desktop and retry step 7

# Shutdown
docker compose down

# Local dev
Change java code and run ./gradlew clean build
If the stack is not already running, start it first with docker compose up -d
Then rebuild only the app container with docker compose up -d --no-deps --build liquidradar-flink
ABOVE DOES NOT RESTART THE WHOLE APP

# Useful commands
terminal:
make sure Kafka is running first with docker compose up -d
check kafka topic message count: docker compose exec kafka kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic coinbase.trade.btcusd.raw
print 10 messages: docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic coinbase.trade.btcusd.raw --from-beginning --max-messages 10
specific message: docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic coinbase.trade.btcusd.raw --partition 0 --offset 500 --max-messages 1
delete data: docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --topic coinbase.trade.btcusd.raw --delete

delete cached docker data (if changed java app): docker compose down --rmi all --volumes