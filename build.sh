#!/bin/bash

echo "Compiling TradeX Distributed System..."

# 1. Compile the Main Matching Engine (Pipeline Server)
echo "-> Building pipeline_server..."
g++ -O3 -std=c++20 -I include \
    tests/pipeline_server.cpp \
    src/oms.cpp \
    src/oms_order_receiving_server.cpp \
    src/producer_consumer.cpp \
    src/trade_ring_buffer.cpp \
    src/symbol_loader.cpp \
    -o pipeline_server \
    -labsl_raw_hash_set -labsl_hash -labsl_city -labsl_raw_logging_internal -labsl_low_level_hash -labsl_throw_delegate -labsl_base \
    -lrt -lpthread

# 2. Compile the Live Trade Processor (Disk Persistence)
echo "-> Building live_tp..."
g++ -O3 -std=c++20 -I include \
    src/live_tp.cpp \
    src/trade_processor.cpp \
    src/trade_ring_buffer.cpp \
    src/spsc_queue.cpp \
    -o live_tp -lrt -lpthread

# 3. Compile the Live Market Feed Reader (UDP Broadcaster)
echo "-> Building live_mfr..."
g++ -O3 -std=c++20 -I include \
    src/live_mfr.cpp \
    src/MarketFeedReader.cpp \
    src/trade_ring_buffer.cpp \
    src/spsc_queue.cpp \
    -o live_mfr -lrt -lpthread

# 4. Compile the Remote Bots (Traffic Generator)
echo "-> Building remote_bots..."
g++ -O3 -std=c++20 -I include \
    bots/run.cpp \
    src/symbol_loader.cpp \
    -o remote_bots -lpthread

echo "All components compiled successfully!"