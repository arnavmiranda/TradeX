#ifndef  ORDER_BOOK_H
#define ORDER_BOOK_H

#include <cstring>
#include <iostream>
#include <cstdint>
#include <queue>
#include <unordered_map>
#include <vector>
#include "order.h"
#include "trade.h"
#include "absl/container/flat_hash_map.h"

namespace matching_engine {
struct OrderNode{
    Order order;
    uint32_t next;
    uint32_t back;
};

//Memory Pool
class MemPool{
    private:
        OrderNode* nodes;
        uint32_t freeHead;
        size_t used;
        size_t capacity;

    public:
        explicit MemPool(size_t cap) : capacity(cap), used(0) {
            nodes = static_cast<OrderNode*>(std::malloc(sizeof(OrderNode) * capacity));

            if(!nodes){
                throw std::bad_alloc();
            }
            freeHead = 0;
            for(size_t i = 0; i<capacity-1; i++){
                nodes[i].next = i+1;
            }
            nodes[capacity-1].next = NULL_IDX;
        }

        ~MemPool(){
            std::free(nodes);
        }

        inline uint32_t allocate(){
            if(freeHead == NULL_IDX) return NULL_IDX;

            uint32_t idx = freeHead;
            freeHead = nodes[idx].next;
            nodes[idx].next = NULL_IDX;
            nodes[idx].back = NULL_IDX;
            used++;
            return idx;
        }

        inline void deallocate(int idx){
            if(idx == NULL_IDX) return;

            nodes[idx].next = freeHead;
            freeHead = idx;
            used--;
        }

        inline OrderNode* getNode(int idx) {
            return &nodes[idx];
        }
};



//Queue for price levels
class PriceQueues {
    private:
        uint32_t head;
        uint32_t tail;
        size_t count;
        bool heap_entry;
        MemPool* pool;
    public: 
        explicit PriceQueues(MemPool* p) : head(NULL_IDX), tail(NULL_IDX), count(0), heap_entry(false), pool(p) {}

        OrderNode* gethead() {return pool->getNode(head);}

        inline bool isEmpty() const {
            return count == 0;
        }

        inline bool has_heap_entry() const {
            return heap_entry;
        }

        inline void set_heap_entry(bool val) {
            heap_entry = val;
        }

        inline int insertOrder(Order& order){
            uint32_t idx = pool->allocate();
            if(idx == NULL_IDX) return NULL_IDX;

            OrderNode* node = pool->getNode(idx); 
            node->order = order;
            if(head == NULL_IDX){
                head = tail = idx;
            }
            else{
                pool->getNode(tail)->next = idx;
                pool->getNode(idx)->back = tail;
                tail = idx;
            }
            count++;
            return idx;
        }


        inline bool removeOrder(int idx){
            if(idx == NULL_IDX) return false;

            OrderNode* node = pool->getNode(idx);
            uint32_t prev = node->back;
            uint32_t next = node->next;

            if(idx == head){
                head = next;
                if(head != NULL_IDX)
                    pool->getNode(head)->back = NULL_IDX;
            }
            else{
                pool->getNode(prev)->next = next;
            }

            if(idx == tail){
                tail = prev;
            }
            else if(next != NULL_IDX){
                pool->getNode(next)->back = prev;
            }

            pool->deallocate(idx);
            count--;
            return true;
        }
};

//something to consider is whether we even require heaps here. the price range is bounded so the cost of iterative scanning is minimal compared to the maintance cost of a heap. could be tested though.
template <typename T, typename Compare>
class ReservablePriorityQueue : public std::priority_queue<T, std::vector<T>, Compare> {
public:
    void reserve(size_t n) {
        this->c.reserve(n);
    }
};

//Order Book
class OrderBook{
    private:
    using BuyPriceHeap = ReservablePriorityQueue<uint64_t, std::less<uint64_t>>;
    using SellPriceHeap = ReservablePriorityQueue<uint64_t, std::greater<uint64_t>>;

        uint64_t lower_limit;
        uint64_t upper_limit;
        size_t PRICE_LEVELS;
        std::vector<PriceQueues> price_levels;
        BuyPriceHeap next_buy_price;
        SellPriceHeap next_sell_price;
        MemPool pool;
        absl::flat_hash_map<uint64_t, uint32_t> orderMap;

        uint64_t default_best_buy_price;
        uint64_t default_best_sell_price;

        uint64_t total_buy_qty;
        uint64_t total_sell_qty;

    public:
        OrderBook(size_t poolSize, uint64_t lower_price, uint64_t upper_price) :
                lower_limit(lower_price),
                upper_limit(upper_price),
                PRICE_LEVELS(upper_price - lower_price + 1),
                pool(poolSize),
                default_best_buy_price(lower_price > 0 ? lower_price - 1 : 0),
                default_best_sell_price(upper_price == UINT64_MAX ? UINT64_MAX : upper_price + 1) {
            
            next_buy_price.reserve(PRICE_LEVELS);
            next_sell_price.reserve(PRICE_LEVELS);
            next_buy_price.push(default_best_buy_price);
            next_sell_price.push(default_best_sell_price);

            price_levels.reserve(PRICE_LEVELS);
            for(size_t i = 0; i < PRICE_LEVELS; i++) {
                price_levels.emplace_back(&pool);
            }
            orderMap.rehash(poolSize);
        }

        inline int priceToIndex(uint64_t price) const {
            return static_cast<int>(price) - lower_limit;  
        }


        inline uint64_t bestbuyprice() { 
            while(next_buy_price.top() != default_best_buy_price && price_levels[priceToIndex(next_buy_price.top())].isEmpty()) [[unlikely]] {
                price_levels[priceToIndex(next_buy_price.top())].set_heap_entry(false);
                next_buy_price.pop();
            }
            return next_buy_price.top();
        }

        inline uint64_t bestsellprice() { 
            while(next_sell_price.top() != default_best_sell_price && price_levels[priceToIndex(next_sell_price.top())].isEmpty()) [[unlikely]] {
                price_levels[priceToIndex(next_sell_price.top())].set_heap_entry(false);
                next_sell_price.pop();
            }
            return next_sell_price.top();
        }

        inline PriceQueues& getpricelevels(int idx) {
            return price_levels[idx];
        }

        // inline auto next_buy_price_available(uint64_t idx) const { return next_buy_price.top() != default_best_buy_price; }
        //i dont think this is used
        
        bool addBuyOrder(Order &order){
            int index = priceToIndex(order.price);
            if(index<0 || index>=PRICE_LEVELS) [[unlikely]] return false;

            bool price_level_is_empty = price_levels[index].isEmpty();
            bool heap_entry = price_levels[index].has_heap_entry();

            int idx = price_levels[index].insertOrder(order);
            if(idx == NULL_IDX) [[unlikely]] return false;
            orderMap[order.order_id] = idx;
            
            price_levels[index].set_heap_entry(true);
            if(price_level_is_empty && !heap_entry) //this is because redundant entries in the heap are not removed until they are encountered as top and found empty in bestbuyprice() or bestsellprice()
                next_buy_price.push(order.price);   //on the other hand, if there was already a heap entry for this price level, we don't need to push it again, as it is already in the heap.
            
            total_buy_qty += order.quantity;
            return true;
        }

        bool addSellOrder(Order &order){
            int index = priceToIndex(order.price);
            if(index<0 || index>=PRICE_LEVELS) [[unlikely]] return false;

            bool price_level_is_empty = price_levels[index].isEmpty();
            bool heap_entry = price_levels[index].has_heap_entry();

            int idx = price_levels[index].insertOrder(order);
            if(idx == NULL_IDX) [[unlikely]] return false;
            orderMap[order.order_id] = idx;

            price_levels[index].set_heap_entry(true);
            if(price_level_is_empty && !heap_entry)
                next_sell_price.push(order.price);
            
            total_sell_qty += order.quantity;
            return true;
        }

        Order* findOrder(uint64_t order_id) {
            auto it = orderMap.find(order_id);
            if(it == orderMap.end()) [[unlikely]] return nullptr;

            return &(pool.getNode(it->second)->order);
        }

        //removes the order at the head of the price level queue for the given price, if it exists
        bool removeBuyOrder(uint64_t price) {
            int idx = priceToIndex(price);
            if(price_levels[idx].isEmpty()) [[unlikely]] return false;
            uint64_t id = price_levels[idx].gethead()->order.order_id;
            uint32_t qty = price_levels[idx].gethead()->order.quantity;
            
            auto it = orderMap.find(id);
            if(it == orderMap.end()) [[unlikely]] return false;

            int index = it->second;
            bool removed = price_levels[idx].removeOrder(index);
            if(removed){
                orderMap.erase(id);
                total_buy_qty -= qty;
                // Heap Entry will be removed in bestbuyprice() when it is encountered as top and found empty
            }
            return removed;
        }

        bool removeSellOrder(uint64_t price) {
            int idx = priceToIndex(price);
            if(price_levels[idx].isEmpty()) [[unlikely]] return false;
            uint64_t id = price_levels[idx].gethead()->order.order_id;
            uint64_t qty = price_levels[idx].gethead()->order.quantity;
            
            auto it = orderMap.find(id);
            if(it == orderMap.end()) [[unlikely]] return false;

            int index = it->second;
            bool removed = price_levels[idx].removeOrder(index); //i dont really know why we're checking this, would it ever be false?
            if(removed){
                orderMap.erase(id);
                total_sell_qty -= qty;
                // Heap Entry will be removed in bestsellprice() when it is encountered as top and found empty
            }
            return removed;
        }

        bool cancelOrder(uint64_t id){
            auto it = orderMap.find(id);
            if(it == orderMap.end()) [[unlikely]] return false;

            int idx = it->second;

            orderMap.erase(it);

            OrderNode* node = pool.getNode(idx);
            int i = priceToIndex(node->order.price);
            bool removed = price_levels[i].removeOrder(idx);
            return removed;
        }

        bool modifyQuantity(uint64_t price, uint32_t new_qty) {
            int idx = priceToIndex(price);
            if(idx < 0 || (size_t)idx >= PRICE_LEVELS) [[unlikely]] return false;
            if(price_levels[idx].isEmpty()) [[unlikely]] return false;
            price_levels[idx].gethead()->order.quantity = new_qty;
            return true;
        }

        uint64_t getTotalBuyQty() const { return total_buy_qty; }
        uint64_t getTotalSellQty() const { return total_sell_qty; }
        uint64_t getTotalQty() const { return total_buy_qty + total_sell_qty; }

        void decreaseTotalBuyQty(uint32_t qty) { total_buy_qty -= qty; };
        void decreaseTotalSellQty(uint32_t qty) { total_sell_qty -= qty; };
        
        inline bool isPriceValid(uint64_t price) const {
            return price >= lower_limit && price <= upper_limit;
        }
        
        ~OrderBook() = default;

};

//note: we're doing static sharding rn so next target should be:
// a) testing with different group counts to find the optimal number, im not sure why its 2 rn
// b) integrating a mechanism that actually tracks the order counts and some stats so that we can:
// c) dynamically shard the order books into groups based on the current load, and reassign them as needed to balance the load across groups.
// d) This would involve monitoring the number of orders and trades per symbol and adjusting the group assignments accordingly.

// //each symbol has its own order book
// class OrderBookManager {
// private:
//     //using pointers of order books because we could construct them out of order and want to call resize
//     std::vector<std::unique_ptr<OrderBook>> books;
//     size_t max_books;

// public:
//     OrderBookManager(size_t number_of_books) : max_books(number_of_books) {
//         books.resize(max_books);
//     }

//     OrderBook& get(uint32_t symbol_id) {
//         if(symbol_id >= max_books || !books[symbol_id]) [[unlikely]]{
//             throw std::out_of_range("Symbol ID exceeds the number of symbols managed.");
//         }
//         return *books[symbol_id];
//     }

//     OrderBook& addBook(uint32_t symbol_id, size_t poolSize, uint64_t lower_price, uint64_t upper_price) {
//         if(symbol_id >= max_books) [[unlikely]]{
//             throw std::out_of_range("Symbol ID exceeds the number of symbols managed.");
//         }
//         books[symbol_id] = std::make_unique<OrderBook>(poolSize, lower_price, upper_price);
//         return *books[symbol_id];
//     }

//     ~OrderBookManager() = default;
// };

class OrderBookManager {
private:
    // Store pointers so the objects themselves never move in memory
    std::vector<std::unique_ptr<OrderBook>> books;
    size_t max_books;

public:
    OrderBookManager(size_t number_of_books) : max_books(number_of_books) {
        // resize() works perfectly here because unique_ptr default-constructs to nullptr
        books.resize(max_books);
    }

    OrderBook& get(uint32_t symbol_id) {
        if(symbol_id >= max_books || !books[symbol_id]) [[unlikely]]{
            throw std::out_of_range("Symbol ID exceeds limits or book not initialized.");
        }
        return *books[symbol_id];
    }


    OrderBook& addBook(uint32_t symbol_id, size_t poolSize, uint64_t lower_price, uint64_t upper_price) {
        if(symbol_id >= max_books) [[unlikely]]{
            throw std::out_of_range("Symbol ID exceeds the number of symbols managed.");
        }
        // Allocate the book on the heap so its memory address is permanently fixed
        books[symbol_id] = std::make_unique<OrderBook>(poolSize, lower_price, upper_price);
        return *books[symbol_id];
    }

    ~OrderBookManager() = default;
};



}
#endif