#ifndef DEMO_BOT_H
#define DEMO_BOT_H

#include "order.h"
#include "symbol_loader.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <random>
#include <string>
#include <utility>
#include <vector>

namespace bots {

enum class ClientOrderType {
    MARKET,
    LIMIT,
    STOP_LOSS,
    ICEBERG
};

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

class DemoBot {
public:
    using SendFn = std::function<bool(const ClientOrderWire&)>;

    DemoBot(std::vector<SymbolInfo> symbols, uint64_t trader_id, SendFn send_fn)
        : symbols_(std::move(symbols)),
          trader_id_(trader_id),
          send_fn_(std::move(send_fn)),
          rng_(std::random_device{}()),
          type_dist_(0, 99),
          symbol_dist_(0, symbols_.empty() ? 0 : symbols_.size() - 1) {}

    bool generateSingleOrder() {
        if (symbols_.empty() || !send_fn_) {
            return false;
        }

        const auto& info = symbols_[symbol_dist_(rng_)];
        ClientOrderWire ord{};
        ord.client_order_id = (trader_id_ << 32) | (rng_() & 0xFFFFFFFFULL);
        std::strncpy(ord.symbol, info.symbol.c_str(), sizeof(ord.symbol) - 1);
        ord.trader_id = trader_id_;
        ord.symbol_id = info.symbol_id;
        ord.quantity = static_cast<uint32_t>((rng_() % 50) + 1);
        ord.filled_quantity = 0;
        ord.is_active = true;
        ord.type = (rng_() % 2 == 0) ? matching_engine::OrderType::BUY : matching_engine::OrderType::SELL;

        std::uniform_int_distribution<uint64_t> price_gen(info.lower_limit + 1, info.upper_limit - 1);
        uint64_t rand_price = price_gen(rng_);
        int roll = type_dist_(rng_);

        if (roll < 70) {
            ord.execution_type = ClientOrderType::LIMIT;
            ord.price = rand_price;
        } else if (roll < 85) {
            ord.execution_type = ClientOrderType::STOP_LOSS;
            ord.trigger_price = rand_price;
            ord.price = (rng_() % 2 == 0) ? rand_price + 2 : rand_price - 2;
        } else if (roll < 95) {
            ord.execution_type = ClientOrderType::ICEBERG;
            ord.price = rand_price;
            ord.display_quantity = std::max<uint32_t>(1, ord.quantity / 5);
        } else {
            ord.execution_type = ClientOrderType::MARKET;
        }

        return send_fn_(ord);
    }

private:
    std::vector<SymbolInfo> symbols_;
    uint64_t trader_id_;
    SendFn send_fn_;
    std::mt19937_64 rng_;
    std::uniform_int_distribution<int> type_dist_;
    std::uniform_int_distribution<std::size_t> symbol_dist_;
};

} // namespace bots

#endif