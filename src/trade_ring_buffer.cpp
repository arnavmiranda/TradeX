#include "trade_ring_buffer.h"
#include "trade.h"
#include <csignal>
#include <cstdint>
#include <cstring>
#include <sys/types.h>

namespace TradeRingBuffer {

    trade_ring_buffer::trade_ring_buffer(bool IsProducer, int buffer_id) :
        index(0),                   // to start from beginning of ring buffer
        next_expected_seq(1),       // first expected sequence number is 1 because 0 is reserved for empty slot
        is_producer(IsProducer),
        buffer_id(buffer_id)
    {
        // Try to create the shared memory object
        int fd = shm_open(filename[buffer_id].data(), O_RDWR | O_CREAT | O_EXCL, 0666);

        if(fd == -1) fd = shm_open(filename[buffer_id].data(), O_RDWR, 0666); // Open existing object
        else {
            if (ftruncate(fd, sizeof(ring_buffer)) == -1) {
            perror("ftruncate");
            close(fd);
        // Handle error: return or throw
    }
        }// Set size of new object

        uint32_t access_flags = PROT_READ;
        if(is_producer) access_flags |= PROT_WRITE;

        // Map the shared memory object to the process address space 
        void * ptr = mmap(0, sizeof(ring_buffer), access_flags, MAP_SHARED, fd, 0);
        close(fd);
        
        rb = static_cast<ring_buffer *>(ptr);
    }

    // // No explicit check needed for producer because connsumers dont have write access
    // // We must first write data and then update seq to ensure the data is safe to read
    // bool trade_ring_buffer::add_trade(matching_engine::Trade &recent_trade) {
    //     item_node & node = rb->ring[index];
    //     std::memcpy(&node.curr_trade, &recent_trade, sizeof(matching_engine::Trade));
    //     node.seq.store(next_expected_seq, std::memory_order_release);
    //     update_index_and_seq();
    //     return true;
    // }


    bool trade_ring_buffer::add_trade(matching_engine::Trade &recent_trade) {
        item_node & node = rb->ring[index];
        
        // 1. Mark as 'writing' (odd sequence)
        uint64_t next_seq = (next_expected_seq << 1); 
        node.seq.store(next_seq | 1, std::memory_order_release);
        std::atomic_thread_fence(std::memory_order_release);
        
        // 2. Write payload
        std::memcpy(&node.curr_trade, &recent_trade, sizeof(matching_engine::Trade));
        
        // 3. Mark as 'done writing' (even sequence)
        node.seq.store(next_seq + 2, std::memory_order_release);
        
        update_index_and_seq();
        return true;
    }

    // If seq matches next_expected_seq, new data is available
    bool trade_ring_buffer::any_new_trade() {
        item_node & node = rb->ring[index];
        uint64_t node_seq = node.seq.load(std::memory_order_acquire);
        return (node_seq >= next_expected_seq); //if greater then it has been lapped
    }

    // If lagged out, seq must be greater than next_expected_seq
    bool trade_ring_buffer::lagged_out() {
        item_node & node = rb->ring[index];
        uint64_t node_seq = node.seq.load(std::memory_order_acquire);
        uint64_t diff = ((node_seq + MOD) - next_expected_seq) & (MOD - 1);
        return (diff >= RING_SIZE);
    }

    // // Copies data directly into provided address
    // // Assumes that address is valid and large enough to hold Trade object
    // // Assumes that any_new_trade() was called before this to ensure new data is available
    // void trade_ring_buffer::get_trade(void * address) {
    //     item_node & node = rb->ring[index];
    //     std::memcpy(address, &node.curr_trade, sizeof(matching_engine::Trade));
    //     update_index_and_seq();
    // }

    // rewrote this & addTrade to ensure that we don't have torn reads, and that the data is safe to read before we copy it
    void trade_ring_buffer::get_trade(void * address) {
        item_node & node = rb->ring[index];
        uint64_t seq1, seq2;
        do {
            seq1 = node.seq.load(std::memory_order_acquire);
            if (seq1 & 1) continue; // Spin if producer is currently writing
            
            std::memcpy(address, &node.curr_trade, sizeof(matching_engine::Trade));
            std::atomic_thread_fence(std::memory_order_acquire);
            seq2 = node.seq.load(std::memory_order_acquire);
        } while (seq1 != seq2); // Retry if torn read detected
        
        update_index_and_seq();
    }

    // Returns a copy of the trade object
    matching_engine::Trade trade_ring_buffer::get_trade() {
        matching_engine::Trade trade_copy;
        get_trade(static_cast<void *>(&trade_copy));
        return trade_copy;
    }

    trade_ring_buffer::~trade_ring_buffer() {
        munmap(static_cast<void *>(rb), sizeof(ring_buffer));
        if(is_producer) {
            shm_unlink(filename[buffer_id].data());
        }
    }

    // Updates index and next_expected_seq after reading a trade
    void trade_ring_buffer::update_index_and_seq() {
        index = (index + 1) & (RING_SIZE - 1);
        next_expected_seq = (next_expected_seq + 1) & (MOD - 1);
        next_expected_seq |= (next_expected_seq == 0);
    }
}