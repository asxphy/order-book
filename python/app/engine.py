import json 
import os 
import time 
from collections import defaultdict, deque 

from confluent_kafka import Consumer, Producer

from .orderbook import MatchingEngine, Side, OrderType 

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","kafka:9092")
GROUP_ID = os.getenv("ENGINE_GROUP",'matching-engine')
ORDERS_IN = os.getenv("ORDERS_IN","orders.in")
EVENTS_OUT = os.getenv("EVENTS_OUT","events.out")

_engines = {}
_dedup = defaultdict(lambda:deque(maxlen=100000))

def get_engine(symbol):
    me = _engines.get(symbol)
    if me is None:
        me = MatchingEngine()
        me.start()
        _engines[symbol] = me 
    return me 


def already_processed(symbol,command_id):
    if not command_id:
        return False
    if command_id in _dedup[symbol]:
        return True 
    
    _dedup[symbol].append(command_id)
    return False 


def make_consumer():
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "enable.auto.commit": False,    
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 300000,
        "fetch.min.bytes": 1_048_576
    }
    return Consumer(conf)
    

def make_producer():
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "enable.idempotence": True,   # idempotent producer
        "linger.ms": 2,
        "batch.num.messages": 10000
    }
    return Producer(conf)

def emit(producer, symbol, event_type,payload):
    producer.produce(
        topic = EVENTS_OUT,
        key = symbol.encode(),
        value = json.dumps({
            "symbol":symbol,
            "event":event_type,
            "payload":payload,
            "ts":time.time()
        }).encode()
    )

def handle_command(prod,cmd):
    symbol = cmd["symbol"]
    ctype = cmd["type"]
    cmd_id = cmd.get("command_id")

    if already_processed(symbol,cmd_id):
        return 
    
    eng = get_engine(symbol)
    if ctype == "LIMIT":
        side = Side.BUY if cmd["side"] == "BUY" else Side.SELL
        qty = int(cmd['quantity'])
        px = float(cmd["price"])
        res = eng.submit_limit(side,qty,px,user_ref=cmd.get("user_ref"))
        emit(prod,symbol,"ACK",{
            "command_id":cmd_id,
            "accepted":True,
            "order_id":res["order_id"],
            "residual_qty":res['residual_qty']
        })

        for t in res["trades"]:
            emit(prod,symbol,"TRADE",{
                "taker_id":t.taker_id,"maker_id":t.maker_id,
                "price":t.price,"quantity":t.quantity
            })

        bb = eng.top_of_book()
        emit(prod,symbol,"BOOK",bb)

    elif ctype == "MARKET":
        side = Side.BUY if cmd["side"] == "BUY" else Side.SELL
        qty  = int(cmd["quantity"])
        res  = eng.submit_market(side, qty, user_ref=cmd.get("user_ref"))
        emit(prod, symbol, "ACK", {
            "command_id": cmd_id,
            "accepted": True,
            "order_id": res["order_id"],
            "filled_qty": res["filled_qty"]
        })
        for t in res["trades"]:
            emit(prod, symbol, "TRADE", {
                "taker_id": t.taker_id, "maker_id": t.maker_id,
                "price": t.price, "quantity": t.quantity
            })
        bb = eng.top_of_book()
        emit(prod, symbol, "BOOK", bb)

    elif ctype == "CANCEL":
        ok = eng.cancel(int(cmd["order_id"]))
        emit(prod, symbol, "ACK", {
            "command_id": cmd_id,
            "accepted": ok,
            "order_id": cmd["order_id"]
        })
        bb = eng.top_of_book()
        emit(prod, symbol, "BOOK", bb)

    else:
        emit(prod, symbol, "ACK", {"command_id": cmd_id, "accepted": False, "error": "unknown command"})


def run():
    consumer = make_consumer()
    producer = make_producer()
    consumer.subscribe([ORDERS_IN])

    try:
        while True: 
            msg = consumer.poll(0.5)
            if msg is None:
                producer.poll(0)
                continue 
            if msg.error():
                raise Exception(msg.error())
            
            cmd = json.loads(msg.value())
            handle_command(producer,cmd)
            consumer.commit(msg,asynchronous=False)
            producer.poll(0)

    except KeyboardInterrupt:
        pass 
    finally:
        consumer.close()
        for eng in _engines.values():
            eng.stop()

if __name__ == "__main__":
    run()
