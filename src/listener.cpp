#include "listener.h"
namespace Client{
    bool spsc_queue::push(uint64_t&& msg)
    {
        uint64_t _head = head.load(std::memory_order_acquire);
        uint64_t _tail = tail.load(std::memory_order_relaxed);

        if (((_tail + 1) & (queue_buffer_size - 1)) != (_head & (queue_buffer_size - 1))) // queue not full
        {
            array[_tail & (queue_buffer_size - 1)] = std::move(msg);
            tail.store(_tail + 1, std::memory_order_release);
            return true;
        }

        return false;
    }

    bool spsc_queue::pop(uint64_t& out_msg)
    {
        uint64_t _head = head.load(std::memory_order_relaxed);
        uint64_t _tail = tail.load(std::memory_order_acquire);

        if (_tail != _head) // queue not empty
        {
            out_msg = array[_head & (queue_buffer_size - 1)];
            head.store(_head + 1, std::memory_order_release);
            return true;
        }

        return false;
    }
    void Listener::init_batch(int capacity)
    {
        batch.capacity = capacity;
        batch.msgs = (struct mmsghdr*) calloc(capacity, sizeof(struct mmsghdr));
        batch.iov = (struct iovec*) calloc(capacity, sizeof(struct iovec));
        batch.buffer = (char*) aligned_alloc(64, MAX_MSG_SIZE * capacity);

        for (int i = 0; i < capacity; i++)
        {
            batch.iov[i].iov_base = batch.buffer + i * MAX_MSG_SIZE;
            batch.iov[i].iov_len = MAX_MSG_SIZE;
            batch.msgs[i].msg_hdr.msg_iov = &batch.iov[i];
            batch.msgs[i].msg_hdr.msg_iovlen = 1; //one message per datagram -> consisting of 23 packets
            batch.msgs[i].msg_hdr.msg_name = nullptr; //stores no address (receiver side)
            batch.msgs[i].msg_hdr.msg_namelen = 0;
        }
    }

    Listener::Listener() : last_seq_num{}
    {
        sockfd_udp = socket(AF_INET, SOCK_DGRAM, 0); // UDP over IPV4
        if (sockfd_udp < 0)
        {
            perror("Error in UDP socket creation in Retransmitter");
            exit(EXIT_FAILURE);
        }

        

        // setting up listening on udp multicast
        addr_udp.sin_port = htons(5000);
        addr_udp.sin_family = AF_INET;
        addr_udp.sin_addr.s_addr = INADDR_ANY; // why am i not listening to just ethernet?
        if (bind(sockfd_udp, (sockaddr *)&addr_udp, sizeof(addr_udp)))
        {
            if (errno == EADDRINUSE)
            {
                // suggested: sleep/retry/fail, need guidance on which
            }
            perror("Issue in binding udp socket");
            exit(EXIT_FAILURE);
        }

        struct ip_mreq mreq;
        
        if (inet_pton(AF_INET, "239.1.1.1", &mreq.imr_multiaddr) != 1)
        {
            perror("inet_pton multicast group");
            exit(EXIT_FAILURE);
        }

        if (inet_pton(AF_INET, "127.0.0.1", &mreq.imr_interface) != 1)
        {
            perror("inet_pton interface");
            exit(EXIT_FAILURE);
        }
        if (setsockopt(sockfd_udp, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0)
        {
            perror("setsockopt failed while setting up membership to udp multicast");
            exit(EXIT_FAILURE);
        }
        uint32_t loop = 1;
        if (setsockopt(sockfd_udp, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop)) < 0) {
            perror("IP_MULTICAST_LOOP");
        }
        sockfd_tcp = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd_tcp < 0) { perror("TCP socket"); exit(EXIT_FAILURE); }

        // Connect to Retransmitter (Server) on 127.0.0.1:8080
        addr_tcp.sin_family = AF_INET;
        addr_tcp.sin_port = htons(8080);
        inet_pton(AF_INET, "127.0.0.1", &addr_tcp.sin_addr);

        std::cout << "[Client] Connecting to Retransmitter TCP..." << std::endl;
        if (connect(sockfd_tcp, (struct sockaddr*)&addr_tcp, sizeof(addr_tcp)) < 0) {
            perror("TCP connect to Retransmitter failed");
            exit(EXIT_FAILURE);
        }
    }

    void Listener::listener()
    {
        init_batch(64);

        while (!done.load(std::memory_order_acquire))
        {
            int ret = recvmmsg(sockfd_udp, batch.msgs, batch.capacity, MSG_DONTWAIT, NULL); // non-blocking batch receive
            if (ret < 0)
            {
                if (errno == EAGAIN || errno == EINTR)
                    continue;

                perror("recvmmsg");
                continue;
            }
            for (int i = 0; i < ret; i++)
            {

                // parsing logic feels fragile
                char *p_msg = (char *)batch.iov[i].iov_base;
                size_t bytes_received = batch.msgs[i].msg_len;
                int msgs_in_packet = bytes_received / sizeof(MarketDataMessage);
                
                for (int count = 0; count < msgs_in_packet; count++)
                {
                    MarketDataMessage *msg = reinterpret_cast<MarketDataMessage *>(p_msg); // trying to get the MarketDataMessage, why am i doing + 64???
                    // std::memcpy(buffer + (msg->seq_num & (SIZE - 1)), msg, sizeof(*msg)); //confused as to what listener will do
                    if (msg->seq_num > last_seq_num + 1)
                    {
                        // Calculate and push missing sequence numbers
                        for (uint64_t missing = last_seq_num + 1; missing < msg->seq_num; ++missing)
                        {
                            queue.push(std::move(missing));
                        }
                    }
                    last_seq_num = msg->seq_num;
                    p_msg += sizeof(MarketDataMessage);
                }
            }
            
        }
    }

    void Listener::missing_packet_request()
    {
        uint64_t seq_num{};
        while (true)
        {
            

            while (!queue.pop(seq_num))
            {
                if (done.load(std::memory_order_relaxed))
                {
                    return;
                    
                }
                continue;
            }

            ssize_t sent = send(sockfd_tcp, &seq_num, sizeof(seq_num), 0); // get just that trade or all 23 trades in a packet???
            if (sent < 0)
            {
                //handle errors
            }
            
            MarketDataMessage recover;
            ssize_t received = recv(sockfd_tcp, &recover, sizeof(MarketDataMessage), MSG_WAITALL);
            // put it in smth
            if (received == 0)
            {
                done.store(true, std::memory_order_release);
            }
        }
    } 
}