#ifndef OMS_H
#define OMS_H

#include <map>
#include <unordered_map>
#include <atomic>
#include <thread>
#include <vector>
#include <chrono>
#include <cstdint>
#include "order.h" 
#include "trade.h"
#include "trade_ring_buffer.h"
#include "../src/producer_consumer.cpp"
#include "absl/container/flat_hash_map.h"

namespace oms {

enum class ClientOrderType {
    MARKET,
    LIMIT,
    STOP_LOSS,
    ICEBERG
};

typedef struct {
    uint64_t client_order_id;
    uint64_t price;
    uint64_t trigger_price;
    char symbol[8];
    uint32_t symbol_id; 
    uint64_t trader_id;
    matching_engine::OrderType type;  
    uint32_t quantity;
    uint32_t display_quantity;
    uint32_t filled_quantity;
    ClientOrderType execution_type;     
    bool is_active;

} ClientOrder;

using InternalOrder = matching_engine::Order;

struct StopLossContainer {
    std::vector<ClientOrder> sell_stops; 
    std::vector<ClientOrder> buy_stops;

    void init(size_t expected_count = 128) {
        sell_stops.reserve(expected_count);
        buy_stops.reserve(expected_count);
    }
};

class OrderManagementSystem {
private:

    std::atomic<bool> shutdown{false};
    static constexpr uint64_t ICEBERG_BIT    = (1ULL << 63);
    int symbolLookupTable[880000]; 
    std::atomic<uint64_t> next_oms_order_id;


    StopLossContainer stop_loss_registry[MAX_SYMBOLS];

    void sendToEngine(const InternalOrder& ord);

    uint64_t getCurrentTimestamp();

    void registerStopLoss(const ClientOrder& ord);
    void checkAndTriggerSL(uint32_t sym_id, uint64_t current_ltp);
   
    int find_id(const std::string& symbol);
    int find_hash(const std::string& symbol);

    uint64_t last_seen_price[MAX_SYMBOLS] = {0};

    struct MarketData { uint64_t last_price; };    

    absl::flat_hash_map<uint64_t, ClientOrder> active_icebergs; 
    absl::flat_hash_map<uint64_t, uint64_t> child_to_parent; // Child ID -> Parent ID

    matching_engine::RingBuffer<ClientOrder> incoming_orders;

    std::atomic<uint64_t> next_client_order_id{1};
    std::atomic<uint64_t> next_child_order_id{ICEBERG_BIT | 1}; 

    matching_engine::MatchingEngineDispatcher* engine;
    std::vector<TradeRingBuffer::trade_ring_buffer*> trade_consumers;

    std::thread oms_thread;

    void send_slice(uint64_t parent_id);
    void check_fill(const matching_engine::Trade& trade);


public:
    explicit OrderManagementSystem(matching_engine::MatchingEngineDispatcher* eng, size_t capacity = 10000);
    ~OrderManagementSystem();

    shared_data::MarketState* shared_memory_ptr = nullptr;
    void listenForClientOrder();
    void start(); 
    void stop();
    bool enqueueClientOrder(const ClientOrder& order);
    uint64_t submit_iceberg_order(uint32_t symbol_id, uint64_t price, matching_engine::OrderType side, uint32_t total_qty, uint32_t display_qty, uint64_t trader_id);
};

}
#endif