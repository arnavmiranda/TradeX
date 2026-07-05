#ifndef ORDER_H
#define ORDER_H

#include <cstring>
#include <iostream>
#include <cstdint>

namespace matching_engine{

    #define MAX_ORDERS_PER_LEVEL 100
    #define SYMBOL_MAX_LENGTH 20
    constexpr uint32_t NULL_IDX = UINT32_MAX;
    #define SYMBOL_BITS 10
    constexpr uint32_t SYMBOL_MASK = (1 << SYMBOL_BITS) - 1; // Mask for lower 10 bits
    #define MAX_SYMBOLS (1<<SYMBOL_BITS)
    #define GROUP_COUNT 2
    #define TOTAL_SYMBOLS (MAX_SYMBOLS*GROUP_COUNT)


    enum class OrderType {
        BUY,
        SELL
    };

    enum class OrderExecutionType{
        MARKET,             
        LIMIT,
        STOP_LOSS,
        ICEBERG
    };

    //Order Structure
    typedef struct {
        uint64_t order_id;
        OrderExecutionType execution_type; //order now contains type
        uint64_t price;
        uint64_t timestamp;
        uint64_t trader_id;
        OrderType type;  
        uint32_t symbol_id;                 // lower  bits for symbol id, upper 22 bits 
        uint32_t quantity;
        uint64_t client_order_id;
    } Order;
}
#endif