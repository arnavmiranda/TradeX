#include "order_book.h"
#include "order.h"
#include <iostream>
#include <chrono>
#include <vector>
#include <iomanip>

using namespace matching_engine;

int main() {
    std::cout << "=== TradeX Core Matching Engine Micro-Benchmark ===\n\n";
    
    const size_t NUM_ORDERS = 5000000; // 5 Million Orders
    
    // Initialize book: 5M pool size, price range 9000 to 11000
    OrderBook book(NUM_ORDERS, 9000, 11000);
    
    // Pre-allocate orders so generation time doesn't skew our metrics
    std::vector<Order> orders(NUM_ORDERS);
    for (size_t i = 0; i < NUM_ORDERS; ++i) {
        orders[i].order_id = i;
        orders[i].price = 10000 + (i % 100); // Distribute across 100 active price levels
        orders[i].quantity = 10;
        orders[i].type = OrderType::BUY;
    }

    std::cout << std::fixed << std::setprecision(2);

    // ---------------------------------------------------------
    // TEST 1: INSERTION (Add Orders)
    // ---------------------------------------------------------
    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < NUM_ORDERS; ++i) {
        book.addBuyOrder(orders[i]);
    }
    auto end = std::chrono::high_resolution_clock::now();
    
    double insert_time = std::chrono::duration<double>(end - start).count();
    double insert_throughput = (NUM_ORDERS / insert_time) / 1e6;
    double insert_latency = (insert_time * 1e9) / NUM_ORDERS;

    std::cout << "[1] Order Insertion (MemPool Allocation + Queue Push)\n";
    std::cout << "    Throughput : " << insert_throughput << " Million orders/sec\n";
    std::cout << "    Avg Latency: " << insert_latency << " ns/op\n\n";

    // ---------------------------------------------------------
    // TEST 2: CANCELLATION (Remove Orders)
    // ---------------------------------------------------------
    start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < NUM_ORDERS; ++i) {
        book.cancelOrder(i);
    }
    end = std::chrono::high_resolution_clock::now();

    double cancel_time = std::chrono::duration<double>(end - start).count();
    double cancel_throughput = (NUM_ORDERS / cancel_time) / 1e6;
    double cancel_latency = (cancel_time * 1e9) / NUM_ORDERS;

    std::cout << "[2] Order Cancellation (Hash Map Lookup + MemPool Free)\n";
    std::cout << "    Throughput : " << cancel_throughput << " Million ops/sec\n";
    std::cout << "    Avg Latency: " << cancel_latency << " ns/op\n\n";

    return 0;
}