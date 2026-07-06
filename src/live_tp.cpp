#include "trade_processor.h"
#include "trade_ring_buffer.h"
#include <vector>
#include <thread>
#include <iostream>

int main() {
    std::cout << "Starting Live Trade Processor (Persistence)..." << std::endl;
    std::vector<TradeProcessor::TradeProcessor*> arr_tp;
    std::vector<std::thread> writers;
    std::vector<std::thread> persisters;

    // IMPORTANT: Make sure the TradeProcessor constructor inside trade_processor.cpp 
    // initializes its internal trBuffer as (false, i) so it acts as a Consumer, not a Producer.
    for (int i = 0; i < TradeRingBuffer::total_ring_buffer_count; i++) {
        std::string file_name = "file_" + std::to_string(i) + "_";
        arr_tp.emplace_back(new TradeProcessor::TradeProcessor(file_name, i));
    }

    for (int i = 0; i < TradeRingBuffer::total_ring_buffer_count; i++) {
        writers.emplace_back([&, i]{ arr_tp[i]->writerThread(); });
        persisters.emplace_back([&, i]{ arr_tp[i]->persistenceThread(); });
    }

    std::cout << "[TP] Attaching to memory mapped files and persisting trades..." << std::endl;
    
    for (auto& t : writers) t.join();
    for (auto& t : persisters) t.join();

    return 0;
}