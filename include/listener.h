#pragma once


// Listens to udp feed as well and queries for lost packets
#include <stdint.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <ifaddrs.h>
#include <string>
#include <atomic>
#include <iostream>

#include "message.h"
#include "spsc_queue.h"

/*TODO:
1) Verify the network models
2) Receive data from UDP
3) Create a buffer to receive dropped packets
4) Currently querying packets is expensive
*/
namespace Client{

    constexpr size_t msg_per_packet = 23;
    constexpr size_t MAX_MSG_SIZE   = msg_per_packet * sizeof(MarketDataMessage);

    typedef struct {
        struct mmsghdr *msgs;
        struct iovec *iov;
        char * buffer;
        int capacity;
    } batch_t;

    #include <atomic>






    constexpr size_t queue_buffer_size{1 << 20}; // arbitrary size
 
    class spsc_queue
    {
    private:
        alignas(64) std::atomic_uint64_t head;
        alignas(64) std::atomic_uint64_t tail;
        uint64_t *array;

    public:
        spsc_queue() : array(new uint64_t[queue_buffer_size]), head(0), tail(0) {}
        bool push(uint64_t &&);
        bool pop(uint64_t &);
        bool empty()
        {
            return head.load(std::memory_order_acquire) == tail.load(std::memory_order_acquire);
        }
    };
    class Listener
    {
        private:
            batch_t batch{};
            int sockfd_udp, sockfd_tcp;
            struct sockaddr_in addr_udp, addr_tcp;
            int addrlen_tcp;
            uint64_t last_seq_num;
            spsc_queue queue;
            std::atomic_bool done{false};
        public:
            Listener();
            void listener();
            void missing_packet_request();
            void init_batch(int capacity);
    };
}