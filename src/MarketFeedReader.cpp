#include "MarketFeedReader.h"
#include <sys/socket.h>
#include <chrono>

/*TODO:
1) Find a way to get ethernet IP address.
2) Come up with a way to deal with deal with lag out
3) Backoff class to deal with spinning on any new trade
*/

// std::atomic_bool done{false};
// std::string get_interface_ip(const std::string& iface_name)
// {
//     struct ifaddrs* ifaddr;
//     getifaddrs(&ifaddr);
//     for (struct ifaddrs* ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next)
//     {
//         if (ifa->ifa_addr->sa_family == AF_INET && iface_name == ifa->ifa_name)
//         {
//             char buf[INET_ADDRSTRLEN];
//             inet_ntop(AF_INET, &((struct sockaddr_in*)ifa->ifa_addr)->sin_addr, buf, sizeof(buf));
//             freeifaddrs(ifaddr);
//             return std::string(buf);
//         }
//     }
//     freeifaddrs(ifaddr);
//     throw std::runtime_error("interface not found: " + iface_name);
// }
namespace MFR{
MarketFeedReader::MarketFeedReader(std::atomic_bool& flag) : flag(flag), seq_num{0}
{
    for (int i = 0; i < TradeRingBuffer::total_ring_buffer_count; i++)
    {
        trb_vec[i] = new TradeRingBuffer::trade_ring_buffer(false, i);
    }
    sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    struct in_addr local_ip;
    // std::string ip = get_interface_ip("eno1");
    if (inet_pton(AF_INET, "127.0.0.1", &local_ip) != 1)
    {
        perror("inet_pton local ip");
        exit(EXIT_FAILURE);
    } // using ethernet or wifi?, sets the ip address according
    setsockopt(sockfd, IPPROTO_IP, IP_MULTICAST_IF, &local_ip, sizeof(local_ip));
    uint32_t loop = 1;
    if (setsockopt(sockfd, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop)) < 0) {
        perror("IP_MULTICAST_LOOP");
    }
    int bufsize = 1 << 20;
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize)); // increases TX (send) buffer size
    addr.sin_family = AF_INET;                                            // operating on IPV4
    addr.sin_port = htons(5000);                                          // listening to port 5000, converts to big endian network order bytes?
    if (inet_pton(AF_INET, "239.1.1.1", &addr.sin_addr) != 1) // was 239.1.1.1
    {
        perror("inet_pton multicast");
        exit(EXIT_FAILURE);
    } // writes the binary address for the multicast group
    
    
    sockfd_tcp = socket(AF_INET, SOCK_STREAM, 0); 
    if (sockfd_tcp == -1) { 
        perror("socket creation failed..."); 
        exit(EXIT_FAILURE); 
    }
    int opt = 1;
    tcp_port = 8000;
    setsockopt(sockfd_tcp, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    servaddr.sin_family = AF_INET; 
    servaddr.sin_addr.s_addr = inet_addr("127.0.0.1"); // ethernet 
    servaddr.sin_port = htons(tcp_port);
    // Binding newly created socket to given IP and verification 
    if ((bind(sockfd_tcp, (struct sockaddr*)&servaddr, sizeof(servaddr))) != 0) { 
        perror("socket bind failed...\n"); 
        exit(0); 
    }
    if ((listen(sockfd_tcp, SOMAXCONN)) != 0) { // for rt alone 
        perror("Listen failed...\n"); 
        exit(0); 
    }
    // client_len = sizeof(client);
    // connect_ = accept(sockfd_tcp, (struct sockaddr*) &client, (unsigned int*)&client_len);
    // // int flags = fcntl(connect_, F_GETFL, 0);
    // // fcntl(connect_, F_SETFL, flags | O_NONBLOCK);
    // if (connect_ < 0)
    // {
    //     perror("Error in connecting to retransmitter");
    //     exit(EXIT_FAILURE);
    // }


}

void MarketFeedReader::init_batch(batch_t *b, int cap)
{
    b->capacity = cap;
    b->msgs = (struct mmsghdr *)calloc(cap, sizeof(struct mmsghdr)); // creates pointer to structs of mmsghdr
    b->iov = (struct iovec *)calloc(cap, sizeof(struct iovec));      // creates pointers to the iovec structs
    b->buffer = (char *)aligned_alloc(64, cap * MAX_MSG_SIZE);       // the actual memory buffer
    b->tcp_buffer = (char *)aligned_alloc(64, cap * MAX_MSG_SIZE);
    b->tcp_capacity = cap * MAX_MSG_SIZE;
    for (int i = 0; i < cap; i++)
    {
        b->iov[i].iov_base = b->buffer + i * MAX_MSG_SIZE; // setting the base ptr for the message in the buffer
        b->iov[i].iov_len = 0;                             // no bytes of messages currently present
        b->msgs[i].msg_hdr.msg_iov = &b->iov[i];           // each mmsghdr gets its a pointer to the iov struct
        b->msgs[i].msg_hdr.msg_iovlen = 1;                 // number of messages in each iovec struct, we have a single message, MarketDataMessage
        b->msgs[i].msg_hdr.msg_name = &addr;               // address of the multicast group
        b->msgs[i].msg_hdr.msg_namelen = sizeof(addr);
    }
}

void MarketFeedReader::readThread()
{
    int trade_counter{};
    MarketDataMessage in;
    
    while (true)
    {
        if (flag == false)
        {
            goto end;
        }

        // Multiplexer: Check every ring buffer sequentially
        for (int i = 0; i < TradeRingBuffer::total_ring_buffer_count; i++)
        {
            // If the buffer has trades, process up to 10 of them at a time 
            // to ensure fair rotation among all groups without getting stuck.
            int reads = 0;
            while (trb_vec[i]->any_new_trade() && reads < 10)
            {
                in = formatMarketData(trb_vec[i]->get_trade());
                queue.push(&in);
                trade_counter++;
                reads++;
            }
        }
    }
    
end:
    done.store(true, std::memory_order_release);
    std::cout << "reader done \n";
}

void MarketFeedReader::sendThread()
{
    batch_t *batch = (batch_t *)malloc(sizeof(batch_t));
    init_batch(batch, 64);
    client_len = sizeof(client);
    connect_ = accept(sockfd_tcp, (struct sockaddr*) &client, (unsigned int*)&client_len);
    // int flags = fcntl(connect_, F_GETFL, 0);
    // fcntl(connect_, F_SETFL, flags | O_NONBLOCK);
    if (connect_ < 0)
    {
        perror("Error in connecting to retransmitter");
        exit(EXIT_FAILURE);
    }
    while (!done.load(std::memory_order_acquire) || !queue.empty())
    {
        int packets_to_send = 0;
        int tcp_offset = 0;

        // Outer loop: Fill up to 'capacity' packets for sendmmsg
        for (int p = 0; p < batch->capacity; ++p)
        {
            char *ptr = (char *)batch->iov[p].iov_base;
            int msgs_packed = 0;

            // Inner loop: Pack up to 23 messages per UDP packet
            while (msgs_packed < (int)msg_per_packet)
            {
                MarketDataMessage msg;
                // CHANGE: Use if (pop) instead of while (!pop) to handle empty queue
                if (queue.pop(msg)) 
                {
                    memcpy(ptr + msgs_packed * sizeof(MarketDataMessage), &msg, sizeof(MarketDataMessage));
                    memcpy(batch->tcp_buffer + tcp_offset, &msg, sizeof(MarketDataMessage));
                    
                    tcp_offset += sizeof(MarketDataMessage);
                    msgs_packed++;
                }
                else 
                {
                    // Queue empty: Stop packing this packet and this batch
                    goto flush; 
                }
            }
            batch->iov[p].iov_len = msgs_packed * sizeof(MarketDataMessage);
            packets_to_send++;
        }

    flush:
        // Handle the "tail" packet that wasn't fully finished in the loop
        if (packets_to_send < batch->capacity) {
            // Check if the current loop was in the middle of packing a packet
            // We need to set the length for that partial packet too
            int current_packet_msgs = (tcp_offset / sizeof(MarketDataMessage)) % msg_per_packet;
            if (current_packet_msgs > 0) {
                batch->iov[packets_to_send].iov_len = current_packet_msgs * sizeof(MarketDataMessage);
                packets_to_send++;
            }
        }

        if (packets_to_send == 0) continue;

        // 1. UDP Send (Multiple Packets)
        int to_send = packets_to_send;
        struct mmsghdr *cursor = batch->msgs;
        while (to_send > 0)
        {
            int sent = sendmmsg(sockfd, cursor, to_send, 0);
            if (sent < 0) {
                if (errno == EINTR) continue;
                perror("sendmmsg");
                goto done_sending;
            }
            cursor += sent;
            to_send -= sent;
        }

        // 2. TCP Send (One contiguous stream matching the UDP data)
        size_t sent_total = 0;
        while (sent_total < (size_t)tcp_offset)
        {
            ssize_t n = send(connect_, batch->tcp_buffer + sent_total, tcp_offset - sent_total, MSG_NOSIGNAL);
            if (n > 0) {
                sent_total += n;
            } else {
                if (errno == EINTR || errno == EAGAIN) continue;
                perror("tcp send");
                goto done_sending;
            }
        }
    }

done_sending:
    std::cout << "exited sender \n";
}

MarketDataMessage MarketFeedReader::formatMarketData(matching_engine::Trade &&trade)
{
    MarketDataMessage out;
    out.price = trade.price;
    out.quantity = trade.quantity;
    out.symbol_id = trade.symbol_id;
    out.timestamp_ns = trade.timestamp_ns;
    out.trade_id = trade.trade_id;
    out.seq_num = seq_num++;
    return out;
}
}
// matching_engine::Trade make_fake_trade(uint64_t i)
// {
//     matching_engine::Trade t;
//     t.price        = 100.0 + (i % 100);
//     t.quantity     = 10 + (i % 50);
//     t.symbol_id    = i % 8;
//     t.timestamp_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();
//     t.trade_id     = i;
//     return t;
// }
// void pusher()
// {
//     TradeRingBuffer::trade_ring_buffer trb(true, 0); // ISSUE
//     matching_engine::Trade trade;
//     for (int i = 0; i < 1'000'960; i++)
//     {
//         trade = make_fake_trade(i);
//         trb.add_trade(trade);
//     }
//     std::cout<< "pusher done \n";
// }
// #include <thread>
// int main(){
//     // MarketFeedReader mfr(0); // ISSUE
//     std::thread push(pusher);
//     push.join();
//     auto tp = std::chrono::steady_clock::now();
//     std::thread sender([&]{ mfr.sendThread(); });
//     std::thread reader([&]{ mfr.readThread(); });
//     sender.join();
//     reader.join();

//     auto tp2 = std::chrono::steady_clock::now();
//     auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(tp2 - tp);
//     std::cout << "Throughput: " << (1'000'960) / static_cast<double>(duration.count()) << "\n";
//     std::cout << "Duration: " << duration.count() << "ms\n";
// }