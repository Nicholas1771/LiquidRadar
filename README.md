# LiquidRadar
Load real time BTCUSD trades from coinbase to snowflake
Flow: Coinbase API -> Kafka -> Snowflake table

# Setup & Run
1. Create snowflake table in snowflake/snowflake_sql_setup.sql
2. Generate key and set the snowflake rsa key
2. Download Docker Desktop
2. In terminal navigate to LiquidRadar/docker
3. Create .env from .env.example with your snowflake user, id, and key
4. gradle clean build
5. Run: docker compose up -d

# Shutdown
docker compose down

# Useful commands
terminal:
check kafka topic message count: docker exec -it docker-kafka-1 kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic coinbase.trade.btcusd.raw
print 10 messages: docker exec -it docker-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic coinbase.trade.btcusd.raw --from-beginning --max-messages 10
specific message: docker exec -it docker-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic coinbase.trade.btcusd.raw --partition 0 --offset 500 --max-messages 1
delete data: docker exec -it docker-kafka-1 kafka-topics --bootstrap-server localhost:9092 --topic coinbase.trade.btcusd.raw --delete

delete cached docker data (if changed java app): docker compose down --rmi all --volumes