#include "order_book.h"
#include "order.h"
#include <iostream>
#include <chrono>
#include <vector>
#include <iomanip>
#include <algorithm>
#include <numeric>

using namespace matching_engine;

// Helper function to calculate and print percentiles
void printPercentiles(std::vector<int64_t>& latencies, const std::string& name) {
    std::sort(latencies.begin(), latencies.end());
    size_t n = latencies.size();
    
    double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);
    double avg = sum / n;

    std::cout << "=== " << name << " Latency (nanoseconds) ===\n";
    std::cout << std::left << std::setw(10) << "Min:"   << latencies[0] << " ns\n";
    std::cout << std::left << std::setw(10) << "p50:"   << latencies[n * 0.50] << " ns (Median)\n";
    std::cout << std::left << std::setw(10) << "p90:"   << latencies[n * 0.90] << " ns\n";
    std::cout << std::left << std::setw(10) << "p99:"   << latencies[n * 0.99] << " ns\n";
    std::cout << std::left << std::setw(10) << "p99.9:" << latencies[n * 0.999] << " ns (Tail Latency)\n";
    std::cout << std::left << std::setw(10) << "Max:"   << latencies[n - 1] << " ns\n";
    std::cout << std::left << std::setw(10) << "Average:" << std::fixed << std::setprecision(2) << avg << " ns\n\n";
}

int main() {
    std::cout << "--- TradeX Micro-Benchmark: Percentile Latency (With Warm-up) ---\n\n";
    
    const size_t NUM_ORDERS = 1000000; // 1 Million Orders for granular timing
    const size_t WARMUP_ORDERS = 200000; // 200k orders to wake up the CPU and caches
    
    // Pre-allocate all test data so we don't trigger page faults during timing
    std::vector<Order> orders(NUM_ORDERS);
    for (size_t i = 0; i < NUM_ORDERS; ++i) {
        orders[i].order_id = i;
        orders[i].price = 10000 + (i % 100); 
        orders[i].quantity = 10;
        orders[i].type = OrderType::BUY;
    }

    std::vector<int64_t> insert_latencies(NUM_ORDERS);
    std::vector<int64_t> cancel_latencies(NUM_ORDERS);

    // =========================================================
    // WARM-UP PHASE (Untimed)
    // =========================================================
    std::cout << "[*] Running warm-up phase (" << WARMUP_ORDERS << " operations)...\n";
    {
        OrderBook warmup_book(WARMUP_ORDERS, 9000, 11000);
        
        // Blast the CPU to wake it up and load the L1/L2 caches
        for (size_t i = 0; i < WARMUP_ORDERS; ++i) {
            warmup_book.addBuyOrder(orders[i]);
        }
        for (size_t i = 0; i < WARMUP_ORDERS; ++i) {
            warmup_book.cancelOrder(i);
        }
        
        // The warmup_book is destroyed here, memory is freed, but the caches are hot.
    }
    std::cout << "[*] Warm-up complete. CPU caches are hot. Starting measurements.\n\n";

    // =========================================================
    // MEASUREMENT PHASE (Timed)
    // =========================================================
    OrderBook book(NUM_ORDERS, 9000, 11000);

    // TEST 1: INSERTION LATENCY
    for (size_t i = 0; i < NUM_ORDERS; ++i) {
        auto start = std::chrono::high_resolution_clock::now();
        
        book.addBuyOrder(orders[i]);
        
        auto end = std::chrono::high_resolution_clock::now();
        insert_latencies[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }
    
    printPercentiles(insert_latencies, "Order Insertion");

    // TEST 2: CANCELLATION LATENCY
    for (size_t i = 0; i < NUM_ORDERS; ++i) {
        auto start = std::chrono::high_resolution_clock::now();
        
        book.cancelOrder(i);
        
        auto end = std::chrono::high_resolution_clock::now();
        cancel_latencies[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

    printPercentiles(cancel_latencies, "Order Cancellation");

    return 0;
}