#include "../src/oms.h"
#include "oms_order_receiving_server.h"
#include "symbol_loader.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <iostream>
#include <thread>
#include <vector>

namespace {

std::atomic<bool> keep_running{true};

void signal_handler(int) {
    keep_running.store(false, std::memory_order_release);
}

} // namespace

int main(int argc, char** argv) {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    const uint16_t port = (argc > 1) ? static_cast<uint16_t>(std::stoi(argv[1])) : 9000;
    const int runtime_seconds = (argc > 2) ? std::stoi(argv[2]) : 20;

    std::vector<SymbolInfo> symbol_data = loadSymbolCSV("data/symbols.csv");
    if (symbol_data.empty()) {
        std::cerr << "Failed to load data/symbols.csv\n";
        return 1;
    }

    uint64_t lower[TOTAL_SYMBOLS] = {0};
    uint64_t upper[TOTAL_SYMBOLS] = {0};
    for (const auto& s : symbol_data) {
        if (s.symbol_id < TOTAL_SYMBOLS) {
            lower[s.symbol_id] = s.lower_limit;
            upper[s.symbol_id] = s.upper_limit;
        }
    }

    matching_engine::MatchingEngineDispatcher engine(65536);
    engine.initialize_engine(lower, upper);

    oms::OrderManagementSystem oms_system(&engine, 131072);
    oms::OrderReceivingServer server(oms_system, port);

    std::thread engine_thread([&engine]() { engine.start(); });
    oms_system.start();
    server.start();

    std::cout << "Pipeline server started on port " << port
              << " for up to " << runtime_seconds << "s\n";

    auto start = std::chrono::steady_clock::now();
    while (keep_running.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        if (runtime_seconds > 0) {
            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - start).count();
            if (elapsed >= runtime_seconds) {
                break;
            }
        }
    }

    std::cout << "Stopping pipeline server...\n";
    server.stop();
    engine.terminate();
    oms_system.stop();

    if (engine_thread.joinable()) {
        engine_thread.join();
    }

    if (oms_system.shared_memory_ptr != nullptr) {
        saveEndOfDayCSV("data/symbols.csv", oms_system.shared_memory_ptr, symbol_data);
    }

    std::cout << "Pipeline server stopped cleanly.\n";
    return 0;
}