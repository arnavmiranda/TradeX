#ifndef OMS_ORDER_RECEIVING_SERVER_H
#define OMS_ORDER_RECEIVING_SERVER_H

#include <atomic>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>

#include "oms.h"

namespace oms {

class OrderReceivingServer {
public:
    OrderReceivingServer(OrderManagementSystem& oms, uint16_t port = 9000);
    ~OrderReceivingServer();

    void start();
    void stop();

private:
    void acceptLoop();
    void clientSession(int client_fd);
    bool readExact(int client_fd, void* buffer, std::size_t size);
    void removeClientFd(int client_fd);

    OrderManagementSystem& oms;
    std::atomic<bool> shutdown{false};
    uint16_t port;
    int server_fd{-1};

    std::thread accept_thread;
    std::mutex client_mutex;
    std::mutex enqueue_mutex;
    std::vector<std::thread> client_threads;
    std::vector<int> active_client_fds;
};

}

#endif