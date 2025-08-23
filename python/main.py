from dataclasses import dataclass 
from collections import defaultdict,deque
from bisect import bisect_left, insort 
import itertools 
from typing import List,Tuple, Optional, Dict 

@dataclass 
class Order:
    id: int
    side: str 
    price: Optional[float]
    qty: int 
    ts: int 
    active: bool = True 

class OrderBook:
    def __init__(self):
        self.bids: List[float] = []
        self.asks: List[float] = []

        self.bid_queues: Dict[float,deque] = defaultdict(deque)
        self.ask_queues: Dict[float,deque] = defaultdict(deque)

        self.bid_qty: Dict[float,int] = defaultdict(int)
        self.ask_qty: Dict[float,int] = defaultdict(int)

        self.orders: Dict[int,Order] = {}

        self._seq = itertools.count(1)
        self._next_id = itertools.count(1)

    def best_bid(self):
        return self.bids[-1] if self.bids else None 

    def best_ask(self):
        return self.asks[0] if self.asks else None 
    

    def add_limit(self,side,price,qty,client_order_id):
        if side not in ('buy','sell'):
            raise ValueError("side must be 'buy' or 'sell'")
        if qty <= 0:
            raise ValueError("qty must be > 0")
        
        oid = next(self._next_id) if client_order_id is None else client_order_id 
        order = Order(id=oid,side=side,price=price,qty=qty,ts=next(self._seq))
        self.orders[oid] = order 

        trades = []
        self._match(order,trades)
        
        if order.qty>0:
            self._enqueue_order(order)

        else:
            order.active = False 

        return oid,trades 
    
    def add_market(self,side,qty,client_order_id):
        if side not in ('buy','sell'):
            raise ValueError("side must be 'buy' or 'sell'")
        if qty<= 0:
            raise ValueError("qty must be > 0")
        
        oid = next(self._next_id) if client_order_id is None else client_order_id
        order = Order(id=oid,side=side,price=None,qty=qty,ts=next(self._seq))
        self.orders[oid] = order 

        trades = []
        self._match(order,trades,market=True)
        order.active = False 

        return oid, trades 
    
    def cancel(self,order_id):
        order = self.orders.get(order_id)
        if not order or not order.active or order.qty==o:
            return False 
        
        if order.side=='buy':
            self.bid_qty[order.price]-=order.qty 

        else:
            self.ask_qty[order.price]-=order.qty 

        order.qty = 0
        order.active =False

        self._remove_price_if_empty(order.side,order.price)
        return True 
    
    def snapshot(self,depth):
        def collect(prices,qty_map,reverse=False):
            out = []
            iterable = prices if not reverse else reversed(prices)
            for p in iterable:
                q = qty_map.get(p,0)
                if q>0:
                    out.append((p,q))
                    if depth and len(out)>= depth:
                        break
            return out 
        
        return {
            'bids': collect(self.bids,self.bid_qty,reverse=True),
            'asks': collect(self.asks,self.ask_qty,reverse=False)
        }
    
    def _insert_price(self,side,price):
        if side =='buy':
            i = bisect_left(self.bids,price)
            if i == len(self.bids) or self.bids[i] != price:
                insort(self.bids,price)
        else:
            i = bisect_left(self.asks,price)
            if i== len(self.asks) or self.asks[i] != price:
                insort(self.asks,price)

    def _enqueue_order(self,order):
        self._insert_price(order.side,order.price)
        if order.side == 'buy':
            self.bid_queues[order.price].append(order.id)
            self.bid_qty[order.price]+=order.qty
        else:
            self.ask_queues[order.price].append(order.id)
            self.ask_qty[order.price] += order.qty 

    def _remove_price_if_empty(self,side,price):
        if side =='buy':
            q = self.bid_queues.get(price)
            if q is None:
                return 
            while q and (not self.order[q[0]].active or self.orders[q[0]].qty==0):
                q.popleft()
            if not q:
                self.bid_queues.pop(price,None)
                self.bid_qty.pop(price,None)
                i = bisect_left(self.bids,price)
                if i<len(self.bids) and self.bids[i]==price:
                    self.bids.pop(i)

        else:
            q = self.ask_queues.get(price)
            if q is None:
                return 
            while q and (not self.orders[q[0]].active or self.order[q[0]].qty==0):
                q.popleft()

            if not q:
                self.ask_queues.pop(price,None)
                self.ask_qty.pop(price,None)
                i = bisect_left(self.asks,price)
                if i<len(self.asks) and self.asks[i] == price:
                    self.asks.pop(i)

    def _crossable(self,taker,best_price):
        if best_price is None:
            return False 
        
        if taker.price is None:
            return True 

        if taker.side == 'buy':
            return taker.price >= best_price 
        else:
            return taker.price <= best_price 
        
    def _match(self,taker,trades,maket=False):
        while taker.qty>0:
            best_price = self.best_ask() if taker.side=='buy' else self.best_bid()
            if not self._crossable(taker,best_price):
                break 

            queue = self.ask_queues[best_price] if taker.side =='buy' else self

            while queue and ((not self.orders[queue[0]].active) or self.orders[queue[0]].qty==0):
                queue.popleft()

            if not queue:
                self._remove_price_if_empty('sell' if taker.side=='buy' else 'buy',best_price)
                continue 

            maker_id = queue[0]
            maker = self.orders[maker_id]
            traded = min(taker.qty,maker.qty)
            trade_price = best_price 

            trades.append((maker.id,taker.id,trade_price,traded))

            taker.qty -= traded 
            maker.qty -= traded 

            if taker.side == 'buy':
                self.ask_qty[best_price]-=traded 
            else:
                self.bid_qty[best_price]-=traded 

            if maker.qty ==0:
                maker.active = False 
                queue.popleft()

            self._remove_price_if_empty('sell' if taker.side=='buy' else 'buy',best_price)

    