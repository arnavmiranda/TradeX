#pragma once

#include "common.h"

#include <poll.h>
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
#include <vector>
#include <iostream>

#include "message.h"
#include "spsc_queue.h"

/*TODO:
1) Write the tcp receiver from mfr on this side
2) Validate that the server client model is correct on this side
3) Handle dropped packets by sending the entire 23 messages. Put a mutex on it?*/

// typedef struct {
//     char            *tcp_buffer;
//     size_t             capacity;
// } batch_t;

// constexpr size_t msg_per_packet = 23;
// constexpr size_t MAX_MSG_SIZE   = msg_per_packet * sizeof(MarketDataMessage);
namespace RT{
constexpr size_t connection_backlog{10}; //what should it be?
static const uint64_t SIZE{1 << 20}; // configure slot sizes to service requests for sequence numbers transmitted 1s ago
class Retransmitter{
    private:
        spsc_queue queue;
        MarketDataMessage *buffer;
        int sockfd_udp;
        int sockfd_tcp_recv;
        int sockfd_tcp_send;
        struct sockaddr_in addr_udp, addr_tcp;
        struct sockaddr_in servaddr, client; 
        socklen_t client_len;
        int connect_;
        int addrlen_tcp;
        batch_t batch;
        char *tcp_buffer; //should be size of sequence number
        std::atomic_bool& flag;
    public:
        Retransmitter(std::atomic_bool& flag);
        void storeThread(); // listenes on MarketFeed's UDP connection 
        void listenerThread(); //separate thread that processes requests by clients for missing messages
        void init_batch(int cap);
};
}