#include <bits/stdc++.h>
using namespace std;

using OrderId   = uint64_t;
using Quantity  = int64_t;
using Price     = int64_t; 

enum class Side { BUY, SELL };
enum class OrderType { LIMIT, MARKET };

struct Trade {
    Price     price;
    Quantity  quantity;
    OrderId   taker_id;
    OrderId   maker_id;
    Side      taker_side;
    int64_t   ts_ns;  
};

static atomic<uint64_t> g_order_id{1};
static atomic<uint64_t> g_arrival_seq{1};

static inline int64_t now_ns() {
    using namespace std::chrono;
    return duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
}

struct Order {
    OrderId    id;
    uint64_t   seq;       
    Side       side;
    OrderType  type;
    Price      price;     
    Quantity   qty;       
    int64_t    ts_ns;     
    string     user_ref;

    Order(Side s, OrderType t, Quantity q, Price p = 0, string uref = {})
        : id(g_order_id.fetch_add(1, memory_order_relaxed)),
          seq(0),
          side(s),
          type(t),
          price(p),
          qty(q),
          ts_ns(now_ns()),
          user_ref(std::move(uref)) {}
};

struct PriceLevel {
    std::list<Order> orders; 
};

struct IndexEntry {
    Side side;
    Price price;
    std::list<Order>::iterator it; 
};

class OrderBook {
public:
    std::map<Price, PriceLevel> asks;
    std::map<Price, PriceLevel> bids;

    std::unordered_map<OrderId, IndexEntry> index;

    vector<Trade> submitMarket(Side side, Quantity qty, const string& user_ref = {});
    pair<vector<Trade>, Quantity> submitLimit(Side side, Quantity qty, Price px, const string& user_ref = {});
    bool cancel(OrderId oid);

    tuple<optional<pair<Price, Quantity>>, optional<pair<Price, Quantity>>> topOfBook() const;
    pair<vector<pair<Price, Quantity>>, vector<pair<Price, Quantity>>> snapshot(size_t depth = 10) const;

private:
    vector<Trade> matchMarket(Order& taker);
    pair<vector<Trade>, Order*> matchLimit(Order& taker); 

    void rest(Order& residual);
    void restOn(map<Price, PriceLevel>& book, Order& residual);

    static Quantity levelQuantity(const PriceLevel& pl) {
        Quantity q = 0;
        for (auto& o : pl.orders) q += o.qty;
        return q;
    }
    static void pruneLevel(map<Price, PriceLevel>& book, typename map<Price, PriceLevel>::iterator itLevel) {
        if (itLevel->second.orders.empty()) book.erase(itLevel);
    }
};


vector<Trade> OrderBook::submitMarket(Side side, Quantity qty, const string& user_ref) {
    Order taker(side, OrderType::MARKET, qty, 0, user_ref);
    taker.seq = g_arrival_seq.fetch_add(1, memory_order_relaxed);
    return matchMarket(taker);
}

pair<vector<Trade>, Quantity> OrderBook::submitLimit(Side side, Quantity qty, Price px, const string& user_ref) {
    Order taker(side, OrderType::LIMIT, qty, px, user_ref);
    taker.seq = g_arrival_seq.fetch_add(1, memory_order_relaxed);

    auto [trades, residualPtr] = matchLimit(taker);
    Quantity residual_qty = residualPtr ? residualPtr->qty : 0;
    if (residualPtr && residual_qty > 0) {
        rest(*residualPtr);
    }
    return { trades, residual_qty };
}

vector<Trade> OrderBook::matchMarket(Order& taker) {
    vector<Trade> trades;
    auto tnow = now_ns();

    if (taker.side == Side::BUY) {
        while (taker.qty > 0 && !asks.empty()) {
            auto itLevel = asks.begin(); 
            Price px = itLevel->first;
            auto& lst = itLevel->second.orders;

            while (taker.qty > 0 && !lst.empty()) {
                auto itMaker = lst.begin(); 
                Order& maker = *itMaker;
                Quantity traded = std::min(taker.qty, maker.qty);
                taker.qty -= traded;
                maker.qty -= traded;

                trades.push_back(Trade{px, traded, taker.id, maker.id, taker.side, tnow});

                if (maker.qty == 0) {
                    
                    index.erase(maker.id);
                    lst.erase(itMaker);
                }
            }
            pruneLevel(asks, itLevel);
        }
    } else {
        
        while (taker.qty > 0 && !bids.empty()) {
            auto itLevel = prev(bids.end()); 
            Price px = itLevel->first;
            auto& lst = itLevel->second.orders;

            while (taker.qty > 0 && !lst.empty()) {
                auto itMaker = lst.begin();
                Order& maker = *itMaker;
                Quantity traded = std::min(taker.qty, maker.qty);
                taker.qty -= traded;
                maker.qty -= traded;

                trades.push_back(Trade{px, traded, taker.id, maker.id, taker.side, tnow});

                if (maker.qty == 0) {
                    index.erase(maker.id);
                    lst.erase(itMaker);
                }
            }
            pruneLevel(bids, itLevel);
        }
    }
    return trades; 
}

pair<vector<Trade>, Order*> OrderBook::matchLimit(Order& taker) {
    vector<Trade> trades;
    auto tnow = now_ns();

    if (taker.side == Side::BUY) {
        
        while (taker.qty > 0 && !asks.empty()) {
            auto itLevel = asks.begin(); 
            Price px = itLevel->first;
            if (px > taker.price) break; 

            auto& lst = itLevel->second.orders;
            while (taker.qty > 0 && !lst.empty()) {
                auto itMaker = lst.begin();
                Order& maker = *itMaker;
                Quantity traded = std::min(taker.qty, maker.qty);
                taker.qty -= traded;
                maker.qty -= traded;

                trades.push_back(Trade{px, traded, taker.id, maker.id, taker.side, tnow});

                if (maker.qty == 0) {
                    index.erase(maker.id);
                    lst.erase(itMaker);
                }
            }
            pruneLevel(asks, itLevel);
        }
    } else {
        
        while (taker.qty > 0 && !bids.empty()) {
            auto itLevel = prev(bids.end()); 
            Price px = itLevel->first;
            if (px < taker.price) break; 

            auto& lst = itLevel->second.orders;
            while (taker.qty > 0 && !lst.empty()) {
                auto itMaker = lst.begin();
                Order& maker = *itMaker;
                Quantity traded = std::min(taker.qty, maker.qty);
                taker.qty -= traded;
                maker.qty -= traded;

                trades.push_back(Trade{px, traded, taker.id, maker.id, taker.side, tnow});

                if (maker.qty == 0) {
                    index.erase(maker.id);
                    lst.erase(itMaker);
                }
            }
            pruneLevel(bids, itLevel);
        }
    }

    
    if (taker.qty > 0) return { trades, &taker };
    return { trades, nullptr };
}

void OrderBook::rest(Order& residual) {
    
    if (residual.side == Side::BUY) restOn(bids, residual);
    else                            restOn(asks, residual);
}

void OrderBook::restOn(map<Price, PriceLevel>& book, Order& residual) {
    auto [itLevel, inserted] = book.try_emplace(residual.price);
    auto& lst = itLevel->second.orders;

    
    lst.push_back(residual);
    auto itOrder = prev(lst.end()); 

    
    itOrder->seq = residual.seq;

    
    index[itOrder->id] = IndexEntry{
        .side  = residual.side,
        .price = residual.price,
        .it    = itOrder
    };
}

bool OrderBook::cancel(OrderId oid) {
    auto it = index.find(oid);
    if (it == index.end()) return false;

    const IndexEntry& e = it->second;
    auto& book = (e.side == Side::BUY) ? bids : asks;

    auto itLevel = book.find(e.price);
    if (itLevel == book.end()) {
        index.erase(it);
        return false;
    }

    auto& lst = itLevel->second.orders;
    
    lst.erase(e.it);
    pruneLevel(book, itLevel);
    index.erase(it);
    return true;
}



tuple<optional<pair<Price, Quantity>>, optional<pair<Price, Quantity>>> OrderBook::topOfBook() const {
    optional<pair<Price, Quantity>> bestBid, bestAsk;

    if (!bids.empty()) {
        auto itB = prev(bids.end());
        bestBid = make_pair(itB->first, levelQuantity(itB->second));
    }
    if (!asks.empty()) {
        auto itA = asks.begin();
        bestAsk = make_pair(itA->first, levelQuantity(itA->second));
    }
    return { bestBid, bestAsk };
}

pair<vector<pair<Price, Quantity>>, vector<pair<Price, Quantity>>>
OrderBook::snapshot(size_t depth) const {
    vector<pair<Price, Quantity>> outBids, outAsks;

    
    for (auto it = bids.rbegin(); it != bids.rend() && outBids.size() < depth; ++it) {
        outBids.emplace_back(it->first, levelQuantity(it->second));
    }
    
    for (auto it = asks.begin(); it != asks.end() && outAsks.size() < depth; ++it) {
        outAsks.emplace_back(it->first, levelQuantity(it->second));
    }
    return { outBids, outAsks };
}

#ifdef DEMO_MAIN
static void print_trades(const vector<Trade>& tr) {
    for (auto& t : tr) {
        cout << "TRADE qty=" << t.quantity << " px=" << t.price
             << " taker=" << t.taker_id << " maker=" << t.maker_id
             << " side=" << (t.taker_side==Side::BUY?"BUY":"SELL") << "\n";
    }
}

int main() {
    OrderBook ob;

    
    auto [t1, r1] = ob.submitLimit(Side::BUY, 10, 10000); 
    auto [t2, r2] = ob.submitLimit(Side::BUY,  5, 10100); 
    auto [t3, r3] = ob.submitLimit(Side::SELL, 8, 10300); 
    auto [t4, r4] = ob.submitLimit(Side::SELL,12, 10400); 

    auto [bb, ba] = ob.topOfBook();
    cout << "TOB: ";
    if (bb) cout << "BID(" << bb->first << "," << bb->second << ") ";
    else cout << "BID(None) ";
    if (ba) cout << "ASK(" << ba->first << "," << ba->second << ")";
    else cout << "ASK(None)";
    cout << "\n";

    
    auto trMkt = ob.submitMarket(Side::BUY, 15);
    print_trades(trMkt);

    
    bool ok = ob.cancel(2);
    cout << "Cancel(2) -> " << (ok?"OK":"NotFound") << "\n";

    
    auto [tr5, r5] = ob.submitLimit(Side::SELL, 10, 10100);
    print_trades(tr5);

    
    auto [sbids, sasks] = ob.snapshot(5);
    cout << "BIDS:\n";
    for (auto& [p,q] : sbids) cout << "  " << p << " -> " << q << "\n";
    cout << "ASKS:\n";
    for (auto& [p,q] : sasks) cout << "  " << p << " -> " << q << "\n";
}
#endif
