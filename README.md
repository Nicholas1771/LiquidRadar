# LiquidRadar
Move live transaction data from coinbase api to s3

# Setup
Replace the below properties in application.properties with your coinbase API keyId and secret

app.coinbase.keyId=YOUR_KEY_ID

app.coinbase.secret=YOU_SECRET

# Start up local Kafka environment with Docker
Download Docker Desktop and run it on local

in docker terminal navigate to docker directory and run below command to bring up kafka

docker compose up -d

# After Kafka is started, start the spring boot app

# Useful commands
terminal:
check kafka topic message count: docker exec -it docker-kafka-1 kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic coinbase.trade.btcusd.raw
print 10 messages: docker exec -it docker-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic coinbase.trade.btcusd.raw --from-beginning --max-messages 10
specific message: docker exec -it docker-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic coinbase.trade.btcusd.raw --partition 0 --offset 500 --max-messages 1
delete data: docker exec -it docker-kafka-1 kafka-topics --bootstrap-server localhost:9092 --topic coinbase.trade.btcusd.raw --delete