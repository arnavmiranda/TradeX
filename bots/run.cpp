#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "order.h"

namespace {

// Must mirror the wire protocol enum values used by the server.
enum class ClientOrderType {
    MARKET,
    LIMIT,
    STOP_LOSS,
    ICEBERG
};

// Must mirror the wire protocol field order and types used by the server.
struct ClientOrderWire {
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
};

struct SymbolInfo {
    std::string symbol;
    uint32_t symbol_id;
    uint64_t ltp;
    uint64_t lower_limit;
    uint64_t upper_limit;
};

struct ValidationResult {
    bool valid;
    std::string reason;
};

std::atomic<bool> keep_running{true};

void signal_handler(int) {
    keep_running.store(false, std::memory_order_release);
}

std::vector<SymbolInfo> load_symbols_csv(const std::string& path) {
    std::vector<SymbolInfo> symbols;

    std::ifstream file(path);
    if (!file.is_open()) {
        return symbols;
    }

    std::string line;
    std::getline(file, line); // header

    while (std::getline(file, line)) {
        if (line.empty()) {
            continue;
        }

        std::stringstream ss(line);
        std::string token;

        SymbolInfo info{};
        std::getline(ss, info.symbol, ',');

        std::getline(ss, token, ',');
        info.symbol_id = static_cast<uint32_t>(std::stoul(token));

        std::getline(ss, token, ',');
        info.ltp = std::stoull(token);

        std::getline(ss, token, ',');
        info.lower_limit = std::stoull(token);

        std::getline(ss, token, ',');
        info.upper_limit = std::stoull(token);

        symbols.push_back(info);
    }

    return symbols;
}

class RemoteOrderClient {
public:
    RemoteOrderClient(const std::string& host, uint16_t port) : host_(host), port_(port) {}

    ~RemoteOrderClient() {
        close_socket();
    }

    bool connect_with_retry(int retries = 50, int sleep_ms = 100) {
        close_socket();

        for (int i = 0; i < retries; ++i) {
            sockfd_ = ::socket(AF_INET, SOCK_STREAM, 0);
            if (sockfd_ < 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
                continue;
            }

            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port = htons(port_);
            if (::inet_pton(AF_INET, host_.c_str(), &addr.sin_addr) != 1) {
                close_socket();
                return false;
            }

            if (::connect(sockfd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
                return true;
            }

            close_socket();
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
        }

        return false;
    }

    bool send_order(const ClientOrderWire& order) {
        const char* ptr = reinterpret_cast<const char*>(&order);
        std::size_t sent = 0;

        while (sent < sizeof(order)) {
            ssize_t n = ::send(sockfd_, ptr + sent, sizeof(order) - sent, 0);
            if (n <= 0) {
                return false;
            }
            sent += static_cast<std::size_t>(n);
        }

        return true;
    }

private:
    void close_socket() {
        if (sockfd_ != -1) {
            ::shutdown(sockfd_, SHUT_RDWR);
            ::close(sockfd_);
            sockfd_ = -1;
        }
    }

    std::string host_;
    uint16_t port_;
    int sockfd_{-1};
};

ValidationResult validate_order(const ClientOrderWire& ord, const SymbolInfo& sym) {
    if (ord.quantity == 0) {
        return {false, "quantity must be > 0"};
    }

    auto in_band = [&](uint64_t px) {
        return px >= sym.lower_limit && px <= sym.upper_limit;
    };

    switch (ord.execution_type) {
        case ClientOrderType::MARKET:
            return {true, "valid market order"};
        case ClientOrderType::LIMIT:
            if (!in_band(ord.price)) {
                return {false, "limit price outside symbol limits"};
            }
            return {true, "valid limit order"};
        case ClientOrderType::STOP_LOSS:
            if (!in_band(ord.trigger_price)) {
                return {false, "trigger price outside symbol limits"};
            }
            if (!in_band(ord.price)) {
                return {false, "stop-loss limit price outside symbol limits"};
            }
            return {true, "valid stop-loss order"};
        case ClientOrderType::ICEBERG:
            if (!in_band(ord.price)) {
                return {false, "iceberg price outside symbol limits"};
            }
            if (ord.display_quantity == 0 || ord.display_quantity > ord.quantity) {
                return {false, "display quantity must be in [1, quantity]"};
            }
            return {true, "valid iceberg order"};
    }

    return {false, "unknown execution type"};
}

ClientOrderWire build_random_order(const SymbolInfo& sym,
                                   uint64_t trader_id,
                                   std::mt19937_64& rng,
                                   std::uniform_int_distribution<int>& type_dist) {
    ClientOrderWire ord{};
    std::strncpy(ord.symbol, sym.symbol.c_str(), sizeof(ord.symbol) - 1);
    ord.symbol_id = sym.symbol_id;
    ord.trader_id = trader_id;
    ord.client_order_id = (trader_id << 32) | (rng() & 0xFFFFFFFFULL);
    ord.quantity = static_cast<uint32_t>((rng() % 200) + 1);
    ord.filled_quantity = 0;
    ord.is_active = true;
    ord.type = (rng() % 2 == 0) ? matching_engine::OrderType::BUY : matching_engine::OrderType::SELL;

    std::uniform_int_distribution<uint64_t> price_gen(sym.lower_limit, sym.upper_limit);
    uint64_t px = price_gen(rng);

    int roll = type_dist(rng);
    if (roll < 65) {
        ord.execution_type = ClientOrderType::LIMIT;
        ord.price = px;
    } else if (roll < 80) {
        ord.execution_type = ClientOrderType::MARKET;
        ord.price = 0;
    } else if (roll < 92) {
        ord.execution_type = ClientOrderType::STOP_LOSS;
        ord.trigger_price = px;
        ord.price = px;
    } else {
        ord.execution_type = ClientOrderType::ICEBERG;
        ord.price = px;
        ord.display_quantity = std::max<uint32_t>(1, ord.quantity / 4);
    }

    return ord;
}

ClientOrderWire maybe_corrupt_order_for_negative_test(const ClientOrderWire& src,
                                                       const SymbolInfo& sym,
                                                       std::mt19937_64& rng,
                                                       int invalid_percent) {
    ClientOrderWire ord = src;
    if (invalid_percent <= 0) {
        return ord;
    }

    int roll = static_cast<int>(rng() % 100);
    if (roll >= invalid_percent) {
        return ord;
    }

    int mode = static_cast<int>(rng() % 4);
    if (mode == 0) {
        ord.quantity = 0;
    } else if (mode == 1) {
        ord.price = sym.upper_limit + 500;
    } else if (mode == 2) {
        ord.execution_type = ClientOrderType::ICEBERG;
        ord.price = sym.lower_limit;
        ord.display_quantity = ord.quantity + 1;
    } else {
        ord.execution_type = ClientOrderType::STOP_LOSS;
        ord.trigger_price = sym.lower_limit - 1;
        ord.price = sym.lower_limit;
    }

    return ord;
}

void run_bot(const std::string& host,
             uint16_t port,
             const std::vector<SymbolInfo>& symbols,
             uint64_t trader_id,
             int orders_per_sec,
             int invalid_percent,
             std::atomic<int>& active_workers,
             std::atomic<uint64_t>& valid_count,
             std::atomic<uint64_t>& invalid_count,
             std::atomic<uint64_t>& sent_count,
             std::atomic<uint64_t>& send_fail_count) {
    active_workers.fetch_add(1, std::memory_order_relaxed);
    RemoteOrderClient client(host, port);
    if (!client.connect_with_retry()) {
        std::cerr << "Bot " << trader_id << " failed to connect to " << host << ':' << port << "\n";
        active_workers.fetch_sub(1, std::memory_order_relaxed);
        return;
    }

    std::mt19937_64 rng(std::random_device{}());
    std::uniform_int_distribution<int> type_dist(0, 99);
    std::uniform_int_distribution<std::size_t> sym_dist(0, symbols.size() - 1);

    const auto sleep_interval = std::chrono::microseconds(1000000 / std::max(1, orders_per_sec));

    while (keep_running.load(std::memory_order_acquire)) {
        const SymbolInfo& sym = symbols[sym_dist(rng)];
        ClientOrderWire base = build_random_order(sym, trader_id, rng, type_dist);
        ClientOrderWire ord = maybe_corrupt_order_for_negative_test(base, sym, rng, invalid_percent);
        ValidationResult vr = validate_order(ord, sym);

        if (!vr.valid) {
            invalid_count.fetch_add(1, std::memory_order_relaxed);
            std::cout << "[BOT " << trader_id << "] INVALID " << sym.symbol << " reason=" << vr.reason << "\n";
        } else {
            valid_count.fetch_add(1, std::memory_order_relaxed);
            if (client.send_order(ord)) {
                sent_count.fetch_add(1, std::memory_order_relaxed);
            } else {
                send_fail_count.fetch_add(1, std::memory_order_relaxed);
                if (!client.connect_with_retry()) {
                    std::cerr << "[BOT " << trader_id << "] reconnect failed\n";
                }
            }
        }

        std::this_thread::sleep_for(sleep_interval);
    }

    active_workers.fetch_sub(1, std::memory_order_relaxed);
}

} // namespace

int main(int argc, char** argv) {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    std::string server_ip = (argc > 1) ? argv[1] : "127.0.0.1";
    uint16_t server_port = (argc > 2) ? static_cast<uint16_t>(std::stoi(argv[2])) : 9000;
    std::string csv_path = (argc > 3) ? argv[3] : "data/symbols.csv";
    int bot_count = (argc > 4) ? std::stoi(argv[4]) : 3;
    int orders_per_sec_per_bot = (argc > 5) ? std::stoi(argv[5]) : 200;
    int invalid_percent = (argc > 6) ? std::stoi(argv[6]) : 5;

    if (bot_count <= 0 || orders_per_sec_per_bot <= 0 || invalid_percent < 0 || invalid_percent > 100) {
        std::cerr << "Usage: ./remote_bots [server_ip] [server_port] [symbols_csv] [bot_count] [orders_per_sec_per_bot] [invalid_percent]\n";
        return 1;
    }

    std::vector<SymbolInfo> symbols = load_symbols_csv(csv_path);
    if (symbols.empty()) {
        std::cerr << "Failed to load symbol rules from " << csv_path << "\n";
        return 1;
    }

    std::cout << "Remote bot runner\n";
    std::cout << "Server=" << server_ip << ':' << server_port << " symbols=" << symbols.size()
              << " bots=" << bot_count << " rate/bot=" << orders_per_sec_per_bot
              << " invalid%=" << invalid_percent << "\n";

    std::atomic<uint64_t> valid_count{0};
    std::atomic<uint64_t> invalid_count{0};
    std::atomic<uint64_t> sent_count{0};
    std::atomic<uint64_t> send_fail_count{0};
    std::atomic<int> active_workers{0};

    std::vector<std::thread> workers;
    workers.reserve(static_cast<std::size_t>(bot_count));

    const uint64_t trader_id_base = 50000;
    for (int i = 0; i < bot_count; ++i) {
        workers.emplace_back(run_bot,
                             server_ip,
                             server_port,
                             std::cref(symbols),
                             trader_id_base + static_cast<uint64_t>(i),
                             orders_per_sec_per_bot,
                             invalid_percent,
                             std::ref(active_workers),
                             std::ref(valid_count),
                             std::ref(invalid_count),
                             std::ref(sent_count),
                             std::ref(send_fail_count));
    }

    while (keep_running.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        std::cout << "stats valid=" << valid_count.load(std::memory_order_relaxed)
                  << " invalid=" << invalid_count.load(std::memory_order_relaxed)
                  << " sent=" << sent_count.load(std::memory_order_relaxed)
                  << " send_fail=" << send_fail_count.load(std::memory_order_relaxed)
                  << '\n';

        if (active_workers.load(std::memory_order_relaxed) == 0) {
            std::cerr << "All bot workers stopped. Exiting.\n";
            keep_running.store(false, std::memory_order_release);
        }
    }

    for (auto& worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    std::cout << "Final stats valid=" << valid_count.load(std::memory_order_relaxed)
              << " invalid=" << invalid_count.load(std::memory_order_relaxed)
              << " sent=" << sent_count.load(std::memory_order_relaxed)
              << " send_fail=" << send_fail_count.load(std::memory_order_relaxed)
              << '\n';

    return 0;
}