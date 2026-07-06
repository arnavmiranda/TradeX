#include "oms.h"
#include <chrono>
#include <iostream>
#include <thread>
#include <algorithm>
#include <mutex>
#include "market_state.h"
#include "symbol_loader.h"

namespace oms {

OrderManagementSystem::OrderManagementSystem(matching_engine::MatchingEngineDispatcher* eng, size_t capacity) 
        : incoming_orders(capacity)
        , engine(eng)
        , shared_memory_ptr(nullptr)
        , next_oms_order_id(1) 
{
    int shm_fd = shm_open("/oms_market_data", O_RDONLY, 0666);
    if (shm_fd != -1) {
        void* ptr = mmap(NULL, sizeof(shared_data::MarketState), PROT_READ, MAP_SHARED, shm_fd, 0);
        
        //Handle MAP_FAILED so it doesn't bypass shutdown checks
        if (ptr == MAP_FAILED) {
            shared_memory_ptr = nullptr;
        } else {
            shared_memory_ptr = static_cast<shared_data::MarketState*>(ptr);
        }
        ::close(shm_fd);
    }
    for(int i = 0; i < GROUP_COUNT; i++) {
        trade_consumers.push_back(new TradeRingBuffer::trade_ring_buffer(false, i));
    }
    uint64_t boot_timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();
    next_oms_order_id.store(boot_timestamp, std::memory_order_relaxed);

    //initialize to -1 to indicate that no symbol is loaded for that hash
    //otherwise garbage value gets routed for invalid numbers and eventually leads to segfaults in the matching engine
    for(int i = 0; i < 880000; i++) {
        symbolLookupTable[i] = -1;
    }

    //symbolLookupTable[34316] = 1;   //for testing AAPL orders
    std::vector<SymbolInfo> symbol_data = loadSymbolCSV("data/symbols.csv");
    for (const auto& s : symbol_data) {
        symbolLookupTable[find_hash(s.symbol)] = s.symbol_id;
    }
}

OrderManagementSystem::~OrderManagementSystem() {
    stop();
    for(auto* t : trade_consumers) {
        delete t;
    }

    if (shared_memory_ptr != nullptr && shared_memory_ptr != MAP_FAILED) {
        munmap(shared_memory_ptr, sizeof(shared_data::MarketState));
        shared_memory_ptr = nullptr;
    }
}

uint64_t OrderManagementSystem::getCurrentTimestamp() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
           std::chrono::high_resolution_clock::now().time_since_epoch()).count();
}

int OrderManagementSystem::find_hash(const std::string& symbol){
    uint32_t hash = 0;

    for(int i = 0; i<4; ++i){
        uint32_t char_val = 0;
        if(i<symbol.length()){
            char_val = (uint32_t)(toupper(symbol[i]) - 'A' + 1);
        }
        hash = (hash<<5) | char_val;
    }
    return hash;
}
int OrderManagementSystem::find_id(const std::string& symbol) {
    uint32_t hash = find_hash(symbol);
    if (hash >= 880000) return -1; 
    return symbolLookupTable[hash];
}

void OrderManagementSystem::start() {
    oms_thread = std::thread(&OrderManagementSystem::listenForClientOrder, this);
}

void OrderManagementSystem::stop() {
    shutdown.store(true, std::memory_order_release);
    if(oms_thread.joinable()) {
        oms_thread.join();
    }
}

bool OrderManagementSystem::enqueueClientOrder(const ClientOrder& order) {
    std::lock_guard<std::mutex> lock(enqueue_mutex);
    return incoming_orders.push(order);
}

void OrderManagementSystem::sendToEngine(const InternalOrder& ord) {
    engine->dispatch_order(ord);
}

void OrderManagementSystem::listenForClientOrder() {
    ClientOrder new_order;

    //NOTE: DO NOT USE CONTINUE WITHIN THIS LOOP: IT LEADS TO DEADLOCKS BECAUSE THE LOOP END IS THE ONLY PLACE WHERE TRADE CONSUMERS ARE POLLED.
    //TRUST ME I SPENT WAY TOO MUCH TIME DEBUGGING THIS. 
    //  the deadlock is between OMS's thread and the matching engine's group threads. 
    //  when the trade ring buffer fills up, the engine threads are blocked waiting to push a trade
    //  but the OMS thread is blocked because the group queues are full and it is not polling the trade ring buffers to consume trades.
    while(!shutdown.load(std::memory_order_relaxed)) {
        InternalOrder out;
        if(incoming_orders.pop(new_order)) {
            
            new_order.symbol_id = find_id(new_order.symbol);

            //drop invalid symbol
            if (new_order.symbol_id != static_cast<uint32_t>(-1)) {
                //ICEBERG
                if (new_order.execution_type == ClientOrderType::ICEBERG) {
                    //std::cout<<"OMS recieved iceberg order"<<new_order.client_order_id<<"\n";
                    active_icebergs[new_order.client_order_id] = new_order;
                    send_slice(new_order.client_order_id);
                }
                //STOP LOSS
                else if (new_order.execution_type == ClientOrderType::STOP_LOSS) {
                    //std::cout<<"OMS recieved SL order"<<new_order.client_order_id<<"\n";
                    registerStopLoss(new_order); 
                } 
                //MARKET
                else if (new_order.execution_type == ClientOrderType::MARKET) {
                    //std::cout<<"OMS recieved market order"<<new_order.client_order_id<<"\n";
                    uint64_t assigned_id = next_oms_order_id.fetch_add(1, std::memory_order_relaxed);
                    out.order_id = assigned_id;
                    out.execution_type = matching_engine::OrderExecutionType::MARKET;
                    out.client_order_id = new_order.client_order_id;
                    out.quantity = new_order.quantity;
                    out.symbol_id = new_order.symbol_id;
                    out.timestamp = getCurrentTimestamp();
                    out.trader_id = new_order.trader_id;
                    out.type = new_order.type;
                    // if (new_order.type == matching_engine::OrderType::BUY) out.price = engine->getUpperLimit(new_order.symbol_id)+1; //should not be required anymore
                    // else out.price = engine->getLowerLimit(new_order.symbol_id)-1;
                    sendToEngine(out); 
                }
                //LIMIT
                else {
                    if(new_order.price != 0) {
                        //std::cout<<"OMS received limit order"<<new_order.client_order_id<<"\n";
                        uint64_t assigned_id = next_oms_order_id.fetch_add(1, std::memory_order_relaxed);
                        out.order_id = assigned_id;
                        out.execution_type = matching_engine::OrderExecutionType::LIMIT;
                        out.client_order_id = new_order.client_order_id;
                        out.quantity = new_order.quantity;
                        out.symbol_id = new_order.symbol_id;
                        out.timestamp = getCurrentTimestamp();
                        out.trader_id = new_order.trader_id;
                        out.type = new_order.type;
                        out.price = new_order.price;
                        sendToEngine(out); 
                    } else {
                        std::cerr << "Received limit order with price 0, dropping order. Client Order ID: " << new_order.client_order_id << "\n";
                    }
                }
            }
        }

        //we want to make sure this is always polled, otherwise we will deadlock if the ring buffer is full and the matching engine is waiting for us to consume trades
        for (int i = 0; i < GROUP_COUNT; i++) {
            while (trade_consumers[i]->any_new_trade()) {
                matching_engine::Trade t = trade_consumers[i]->get_trade();
                check_fill(t);
            }
        }

        //checks for changes in last traded price and triggers SL
        if(shared_memory_ptr){
            for (uint32_t i=0; i<MAX_SYMBOLS; i++) {
                uint64_t current_ltp = shared_memory_ptr->last_price[i].load(std::memory_order_relaxed);
                if (current_ltp != last_seen_price[i]) {
                    checkAndTriggerSL(i, current_ltp);
                    last_seen_price[i] = current_ltp;
                }
            }
        }
        
    }
}

//STOP LOSS Processing
auto MaxTrigger = [](const ClientOrder& a, const ClientOrder& b) {
    return a.trigger_price < b.trigger_price; 
};

auto MinTrigger = [](const ClientOrder& a, const ClientOrder& b) {
    return a.trigger_price > b.trigger_price; 
};

void OrderManagementSystem::registerStopLoss(const ClientOrder& ord) {   

    auto& container = stop_loss_registry[ord.symbol_id & matching_engine::SYMBOL_MASK];
    if (container.sell_stops.capacity() == 0) container.init();

    ClientOrder stop_ord = ord;
    if (ord.type == matching_engine::OrderType::BUY) {
        container.buy_stops.push_back(stop_ord);
        std::push_heap(container.buy_stops.begin(), container.buy_stops.end(), MinTrigger);
    }
    else {
        container.sell_stops.push_back(stop_ord);
        std::push_heap(container.sell_stops.begin(), container.sell_stops.end(), MaxTrigger);
    }
}

void OrderManagementSystem::checkAndTriggerSL(uint32_t sym_id, uint64_t current_ltp) {
    auto& container = stop_loss_registry[sym_id];

    //Trigger all the orders with trigger price greater than current price
    while (!container.sell_stops.empty() && current_ltp <= container.sell_stops.front().trigger_price) {
        
        ClientOrder triggered_client_ord = container.sell_stops.front();
        std::pop_heap(container.sell_stops.begin(), container.sell_stops.end(), MaxTrigger);
        container.sell_stops.pop_back();

        matching_engine::Order engine_ord;
        uint64_t assigned_id = next_oms_order_id.fetch_add(1, std::memory_order_relaxed);
        engine_ord.order_id = assigned_id;
        engine_ord.client_order_id = triggered_client_ord.client_order_id;
        engine_ord.price = triggered_client_ord.price; 
        engine_ord.symbol_id = find_id(triggered_client_ord.symbol);
        engine_ord.timestamp = getCurrentTimestamp();
        engine_ord.trader_id = triggered_client_ord.trader_id;
        engine_ord.type = triggered_client_ord.type;
        engine_ord.quantity = triggered_client_ord.quantity;
        engine_ord.execution_type = matching_engine::OrderExecutionType::LIMIT;

        //send the order to matching engine as limit order
        sendToEngine(engine_ord);
        //printf("Order triggered\n");
    }

    //Trigger all buy orders with trigger price less than current price
    while (!container.buy_stops.empty() && current_ltp >= container.buy_stops.front().trigger_price) {
        
        ClientOrder triggered_client = container.buy_stops.front();
        std::pop_heap(container.buy_stops.begin(), container.buy_stops.end(), MinTrigger);
        container.buy_stops.pop_back();

        matching_engine::Order engine_ord;
        uint64_t assigned_id = next_oms_order_id.fetch_add(1, std::memory_order_relaxed);
        engine_ord.order_id = assigned_id;
        engine_ord.client_order_id = triggered_client.client_order_id;
        engine_ord.price = triggered_client.price; 
        engine_ord.symbol_id = find_id(triggered_client.symbol);
        engine_ord.timestamp = getCurrentTimestamp();
        engine_ord.trader_id = triggered_client.trader_id;
        engine_ord.type = triggered_client.type;
        engine_ord.quantity = triggered_client.quantity;
        engine_ord.execution_type = matching_engine::OrderExecutionType::LIMIT;

        //send the order to matching engine as limit order
        sendToEngine(engine_ord);
    }
}

 /*       //ICEBERG Processing
uint64_t OrderManagementSystem::submit_iceberg_order(uint32_t symbol_id, uint64_t price, matching_engine::OrderType side, uint32_t total_qty, uint32_t display_qty, uint64_t trader_id) {
    ClientOrder order;
    order.client_order_id = next_client_order_id.fetch_add(1, std::memory_order_relaxed);
    order.symbol_id = symbol_id;
    order.price = price;
    order.type = side;
    order.quantity = total_qty;
    order.display_quantity = display_qty;
    order.filled_quantity = 0;
    order.trader_id = trader_id;
    order.execution_type = ClientOrderType::ICEBERG;
    order.is_active = true;

    while(!incoming_orders.push(order)) { // Pushing into queue for oms thread to pick up
    }
    return order.client_order_id;
} */

void OrderManagementSystem::send_slice(uint64_t parent_id) {
    ClientOrder& parent = active_icebergs[parent_id];
    if(!parent.is_active) return;

    uint32_t remaining = parent.quantity - parent.filled_quantity;
    if(remaining == 0) {
        parent.is_active = false;
        return;
    }

    uint32_t slice_qty = std::min(parent.display_quantity, remaining);
    uint64_t child_id = next_child_order_id.fetch_add(1, std::memory_order_relaxed);

    // Map child back to parent so we can track fills
    child_to_parent[child_id] = parent_id;
    InternalOrder child_order;
    child_order.order_id = child_id;
    child_order.client_order_id = child_id;
    child_order.execution_type = matching_engine::OrderExecutionType::LIMIT;
    child_order.symbol_id = parent.symbol_id;
    child_order.price = parent.price;
    child_order.quantity = slice_qty;
    child_order.type = parent.type;
    child_order.trader_id = parent.trader_id;
    //idk why this wasnt there before but im adding it:
    child_order.timestamp = getCurrentTimestamp();
    
    engine->dispatch_order(child_order);
    //std::cout<<"One slice sent\n";
}

void OrderManagementSystem::check_fill(const matching_engine::Trade& trade) {
    // If neither side is an iceberg, ignore it
    if (!(trade.buy_order_id & ICEBERG_BIT) && !(trade.sell_order_id & ICEBERG_BIT)) return;

    // lambda to process one side of the trade
    auto process_iceberg_leg = [&](uint64_t child_id, uint32_t traded_qty) {
        if (child_to_parent.count(child_id)) {
            uint64_t parent_id = child_to_parent[child_id];
            ClientOrder& parent = active_icebergs[parent_id];
            
            parent.filled_quantity += traded_qty;
            
            bool slice_filled = (parent.filled_quantity % parent.display_quantity == 0) || 
                                (parent.filled_quantity == parent.quantity);

            if(slice_filled) {
                child_to_parent.erase(child_id);
                send_slice(parent_id);
            }
        }
    };

    // Process both legs independently!
    if (trade.buy_order_id & ICEBERG_BIT) {
        process_iceberg_leg(trade.buy_order_id, trade.quantity);
    }
    
    if (trade.sell_order_id & ICEBERG_BIT) {
        process_iceberg_leg(trade.sell_order_id, trade.quantity);
    }
}
} 