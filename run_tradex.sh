#!/bin/bash

echo "Starting TradeX Distributed Environment..."

# 1. Setup Environment
# Wipe out old binary trade logs to avoid stale parsing bugs
rm -rf file_output/*
mkdir -p file_output

# Attempt to add the multicast route (fails silently if it already exists)
sudo ip route add 224.0.0.0/4 dev lo 2>/dev/null || true

# 2. Launch Background Microservices
echo "Starting Trade Processor (Disk Persistence)..."
./live_tp &
TP_PID=$!

echo "Starting Market Feed Reader (UDP Broadcast)..."
./live_mfr &
MFR_PID=$!

# 3. Unblock the Market Feed Reader's TCP Accept
sleep 1
nc 127.0.0.1 8000 > /dev/null &
NC_PID=$!

# 4. Launch the Main Pipeline Server
echo "Starting Matching Engine Pipeline Server..."
./pipeline_server 9000 10 &
SERVER_PID=$!

# Wait a moment for the server to bind to port 9000
sleep 2

# 5. Launch Traffic Bots in the foreground
echo "Launching Remote Bots..."
./remote_bots 127.0.0.1 9000 data/symbols.csv 3 200 1

# 6. Cleanup Protocol (When bots finish or you press Ctrl+C)
echo "Shutting down all microservices..."
kill $TP_PID $MFR_PID $NC_PID $SERVER_PID 2>/dev/null
echo "TradeX environment safely closed."