from kafka import KafkaConsumer


consumer = KafkaConsumer(
  bootstrap_servers=["localhost:9092"],
  group_id="demo-group",
  auto_offset_reset="earliest",
  enable_auto_commit=False,
  consumer_timeout_ms=1000
)
result ={}
consumer.subscribe("stock-updates")

try:
    for message in consumer:
        data=message.value
        result.setdefault(data["symbol",{"weighted_price":0,"total_volume":0}])
        result[data["symbol"]]["weighted_price"]+= data["price"]* data["volume"]
        result[data["symbol"]]["total_volume"]+= data["total_volume"]
        print(message.offset)
              
except Exception as e:
    print(f"Error occurred while consuming messages: {e}")
finally:
    print(result)
    consumer.close()
