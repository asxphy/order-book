import json, os, time, uuid, random 
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","kafka:9092")
p = Producer({"bootstrap.servers":BOOTSTRAP,"linger.ms":1,"batch.num.messages":1000})

symbols = ["AAPL","MSFT","GOOG","TSLA"]

def send(cmd):
    print(cmd)
    key = cmd['symbol'].encode()
    p.produce("orders.in",key=key,value=json.dumps(cmd).encode("utf-8"))


for i in range(1000):
    sym = random.choice(symbols)
    side = random.choice(["BUY","SELL"])
    cmd = {
        "command_id": str(uuid.uuid4()),
        "symbol": sym,
        "type": "LIMIT",
        "side": side,
        "price": round(random.uniform(90,110), 2),
        "quantity": random.randint(1, 100),
        "order_id": None,
        "user_ref": "sim",
        "ts": time.time()
    }
    send(cmd)
    if i % 50 == 0:
        print("sent", i)
    p.poll(0)

p.flush()
print("done")