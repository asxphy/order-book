import time 
import threading
import itertools
from collections import deque, namedtuple 
from concurrent.futures import Future 


class SortedPriceLevels:

    def __init__(self):
        self._prices = []
        self._map = {}

    def __len__(self):
        return len(self._map)
    
    def __contains__(self,price):
        return price in self._map
    
    def keys(self):
        return list(self._prices)
    
    def items(self):
        for p in self._prices:
            yield (p,self._map[p])

    def get(self,price,default=None):
        return self._map.get(price,default)
    
    def __getitem__(self,price):
        return self._map[price]
    
    def __setitem__(self,price,value):
        if price not in self._map:
            import bisect 
            bisect.insort(self._prices,price)
        self._map[price] = value 

    def pop(self,price):
        if price in self._map:
            val = self._map.pop(price)
            import bisect 
            i = None 
            if self._prices and (self._prices[0]==price or self._prices[-1]==price):
                if self._prices[0] == price:
                    self._prices.pop(0)
                else:
                    self._prices.pop()

            else: 
                i = bisect.bisect_left(self._prices,price)
                if i<len(self._prices) and self._prices[i] == price:
                    self._prices.pop(i)
            return val 
        raise KeyError(price)
    
    def peek_min(self):
        if not self._prices:
            return None 
        p = self._prices[0]
        return (p,self._map[p])

    def peek_max(self):
        if not self._prices:
            return None 
        p = self._prices[-1]
        return (p,self._map[p])
    
_order_id_gen = itertools.count(1)
_seq_num_gen = itertools.count(1)

class Side:
    BUY = "BUY"
    SELL = "SELL"

class OrderType:
    LIMIT = "LIMIT"
    MARKET = "MARKET"

class Order:
    __slots__ = ("id", "seq", "side", "quantity", "price", "type", "timestamp", "user_ref")
    def __init__(self, side, quantity, price=None, order_type=OrderType.LIMIT, user_ref=None):
        self.id = next(_order_id_gen)
        self.seq = None  # set by engine for strict time priority
        self.side = side
        self.quantity = int(quantity)
        self.price = float(price) if price is not None else None
        self.type = order_type
        self.timestamp = time.time()
        self.user_ref = user_ref  # optional (e.g., client tag)

    def __repr__(self):
        return (f"Order(id={self.id}, side={self.side}, qty={self.quantity}, "
                f"price={self.price}, type={self.type}, seq={self.seq})")


Trade = namedtuple("Trade", "price quantity taker_id maker_id taker_side ts")

class OrderBook:
    def __init__(self):
        self.asks = SortedPriceLevels()
        self.bids = SortedPriceLevels()

        self.order_index = {}

    def best_ask(self):
        item = self.asks.peek_min()
        return item 

    def best_bids(self):
        item = self.bids.peek_max()
        return item 
    
    def add_limit(self,order):
        trades = []

        if order.side == Side.BUY:
            trades,residual = self._match(order,self.asks,take_best_from_min=True)
            if residual and residual.quantity >0:
                self._rest(self.bids,residual)
            
        else:
            trades,residual = self._match(order,self.bids,take_best_from_min=False)
            if residual and residual.quantity>0:
                self._rest(self.asks,residual)
        return trades,order.quantity
    
    def add_market(self,order):
        trades = []
        if order.side == Side.BUY:
            trades, _ = self._match(order,self.asks,take_best_from_min=True,ignore_price=True)
        else:
            trades, _ = self._match(order,self.bids,take_best_from_min=False,ignore_price=True)

        return trades
    
    def cancel(self,order_id):
        info = self.order_index.pop(order_id,None)
        if not info:
            return False 
        book,price,order = info 
        q = book[price]
        try: 
            q.remove(order)
        except ValueError:
            pass 
        if not q:
            book.pop(price)

        return True 

    def top_of_book(self):
        bb = self.best_bids()
        ba = self.best_ask()

        if bb:
            bp,bq = bb[0], sum(o.quantity for o in bb[1])
        else:
            bp,bq = None,0
        if ba:
            ap,aq = ba[0], sum(o.quantity for o in ba[1])
        else:
            ap,aq = None,0 
        return bp,bq,ap,aq 
    
    def snapshot(self,depth=10):
        bids = []
        asks = []

        items = list(self.bids.items())[::-1]
        for price,q in items[:depth]:
            bids.append((price,sum(o.quantity for o in q)))
        
        items = list(self.asks.items())
        for price,q in items[:depth]:
            asks.append((price,sum(o.quantity for o in q)))

        return {"bids": bids,"asks": asks}
    
    def _rest(self,book,order):
        if order.price not in book:
            book[order.price] = deque()
        book[order.price].append(order)
        self.order_index[order.id] = (book,order.price,order)

    def _take_best_price_leve(self,opposite,from_min=True):
        price,q = (opposite.peek_min() if from_min else opposite.peek_max())
        return price,q
    
    def _match(self,taker,opposite,take_best_from_min,ignore_price=False):
        trades = []
        while taker.quantity>0 and len(opposite)>0:
            price, q = self._take_best_price_leve(opposite,from_min=take_best_from_min)

            if not ignore_price and taker.type==OrderType.LIMIT:
                if taker.side == Side.BUY and price>taker.price:
                    break 
                if taker.side == Side.SELL and price<taker.price:
                    break 

            while taker.quantity>0 and q:
                maker = q[0]
                traded = min(taker.quantity,maker.quantity)
                taker.quantity -= traded 
                maker.quantity -= traded 

                trades.append(Trade(price=price,quantity=traded,taker_id=taker.id,maker_id=maker.id,taker_side=taker.side,ts=time.time()))

                if maker.quantity == 0:
                    q.popleft()
                    self.order_index.pop(maker.id,None)

            if not q:
                opposite.pop(price)

        return trades,(taker if taker.quantity> 0 else None)
    
class MatchingEngine:
    def __init__(self):
        self._book = OrderBook()
        self._cmd_q = deque()
        self._cv = threading.Condition()
        self._running = False 
        self._thread = None

    def start(self):
        with self._cv:
            if self._running:
                return 
            self._running = True 
            self._thread = threading.Thread(target=self._run,name="MatchingEngine",daemon=True)
            self._thread.start()

    def stop(self):
        fut = Future()
        self._enqueue(("STOP",(),{},fut))
        fut.result(timeout=5)

    def submit_limit(self,side,quantity,price,user_ref=None):
        fut = Future()
        self._enqueue(("LIMIT",(side,quantity,price,user_ref),{},fut))
        return fut.result()
    
    def submit_market(self,side,quantity,user_ref=None):
        fut = Future()
        self._enqueue(("MARKET",(side,quantity,user_ref),{},fut))
        return fut.result()
    
    def cancel(self,order_id):
        fut = Future()
        self._enqueue(("CANCEL",(order_id,),{},fut))
        return fut.result()
    
    def top_of_book(self):
        fut = Future()
        self._enqueue(("TOB",(),{},fut))
        return fut.result()
    
    def snapshot(self,depth=10):
        fut = Future()
        self._enqueue(("SNAP",(depth,),{},fut))
        return fut.result()
    
    def _enqueue(self,cmd):
        with self._cv:
            self._cmd_q.append(cmd)
            self._cv.notify()

    def _run(self):
        while True:
            with self._cv:
                while not self._cmd_q:
                    self._cv.wait()
                cmd,args,kwargs,fut = self._cmd_q.popleft()

            try:
                if cmd == "STOP":
                    fut.set_result(True)
                    break

                elif cmd == "LIMIT":
                    side, qty, price, user_ref = args
                    order = Order(side=side, quantity=qty, price=price, order_type=OrderType.LIMIT, user_ref=user_ref)
                    order.seq = next(_seq_num_gen)
                    trades, residual_qty = self._book.add_limit(order)
                    fut.set_result({
                        "order_id": order.id,
                        "trades": trades,
                        "residual_qty": residual_qty
                    })

                elif cmd == "MARKET":
                    side, qty, user_ref = args
                    order = Order(side=side, quantity=qty, price=None, order_type=OrderType.MARKET, user_ref=user_ref)
                    order.seq = next(_seq_num_gen)
                    trades = self._book.add_market(order)
                    filled = sum(t.quantity for t in trades)
                    fut.set_result({
                        "order_id": order.id,
                        "trades": trades,
                        "filled_qty": filled
                    })

                elif cmd == "CANCEL":
                    (order_id,) = args
                    ok = self._book.cancel(order_id)
                    fut.set_result(ok)

                elif cmd == "TOB":
                    bp, bq, ap, aq = self._book.top_of_book()
                    fut.set_result({"best_bid": (bp, bq), "best_ask": (ap, aq)})

                elif cmd == "SNAP":
                    (depth,) = args
                    fut.set_result(self._book.snapshot(depth=depth))

                else:
                    fut.set_exception(RuntimeError(f"Unknown command {cmd}"))

            except Exception as e:
                try:
                    fut.set_exception(e)
                except Exception:
                    pass

if __name__ == "__main__":
    eng = MatchingEngine()
    eng.start()

    # Seed book
    print(">> Seeding book with limits")
    print(eng.submit_limit(Side.BUY, 10, 100.0, user_ref="u1"))
    print(eng.submit_limit(Side.BUY, 5, 101.0, user_ref="u2"))
    print(eng.submit_limit(Side.SELL, 8, 103.0, user_ref="u3"))
    print(eng.submit_limit(Side.SELL, 12, 104.0, user_ref="u4"))
    print("TOB:", eng.top_of_book())
    print("SNAP:", eng.snapshot())

    # Concurrent clients placing orders
    def client_actions(name):
        r1 = eng.submit_limit(Side.BUY, 7, 102.0, user_ref=name)
        r2 = eng.submit_market(Side.SELL, 10, user_ref=name)
        r3 = eng.submit_limit(Side.SELL, 6, 101.0, user_ref=name)  # may cross
        return (r1, r2, r3)

    results = []
    threads = []
    for i in range(4):
        t = threading.Thread(target=lambda idx=i: results.append(client_actions(f"cli{idx}")))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()

    print("\n>> Concurrent client results")
    for r in results:
        print(r)

    print("\nTOB:", eng.top_of_book())
    print("SNAP:", eng.snapshot(depth=5))

    # Cancel an existing resting order (example: cancel order_id=2)
    print("\nCancel 2:", eng.cancel(2))
    print("SNAP:", eng.snapshot(depth=5))

    eng.stop()
