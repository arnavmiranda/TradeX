#include "retransmitter.h"

#include <poll.h>
#include <vector>

/*TODO:
1) Get ip for wifi and ethernet
2) Test
3) Find a better way to handle all setup errors*/
namespace RT {
std::atomic_bool done{false};
// std::string get_interface_ip(const std::string& iface_name)
// {
//     struct ifaddrs* ifaddr;
//     getifaddrs(&ifaddr);f
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
Retransmitter::Retransmitter(std::atomic_bool& flag ) : flag(flag), buffer(new MarketDataMessage[SIZE]), tcp_buffer(new char[1 << 8])
{
    init_batch(64);
    // sockfd_udp = socket(AF_INET, SOCK_DGRAM, 0); // UDP over IPV4
    // if (sockfd_udp < 0)
    // {
    //     perror("Error in UDP socket creation in Retransmitter");
    //     exit(EXIT_FAILURE);
    // }

    sockfd_tcp_recv = socket(AF_INET, SOCK_STREAM, 0); // TCP over IPV4
    if (sockfd_tcp_recv < 0)
    {
        perror("Error in TCP socket creation in Retransmitter to MFR");
        exit(EXIT_FAILURE);
    }
    sockfd_tcp_send = socket(AF_INET, SOCK_STREAM, 0); // TCP over IPV4
    if (sockfd_tcp_send < 0)
    {
        perror("Error in TCP socket creation in Retransmitter to Listener");
        exit(EXIT_FAILURE);
    }

    // // setting up listening on udp multicast
    // addr_udp.sin_port = htons(5000);
    // addr_udp.sin_family = AF_INET;
    // addr_udp.sin_addr.s_addr = INADDR_ANY; // why am i not listening to just ethernet?
    // if (bind(sockfd_udp, (sockaddr *)&addr_udp, sizeof(addr_udp)))
    // {
    //     if (errno == EADDRINUSE)
    //     {
    //         // suggested: sleep/retry/fail, need guidance on which
    //     }
    //     perror("Issue in binding udp socket");
    //     exit(EXIT_FAILURE);
    // }

    // struct ip_mreq mreq;
    // mreq.imr_multiaddr.s_addr = inet_addr("239.1.1.1"); // subscribing to the multicast group
    // // std::string ip = get_interface_ip("en1"); //idk??
    // mreq.imr_interface.s_addr = inet_addr("10.50.59.247");  // over ethernet
    // if (inet_pton(AF_INET, "239.1.1.1", &mreq.imr_multiaddr) != 1)
    // {
    //     perror("inet_pton multicast group");
    //     exit(EXIT_FAILURE);
    // }

    // if (inet_pton(AF_INET, "10.50.59.247", &mreq.imr_interface) != 1)
    // {
    //     perror("inet_pton interface");
    //     exit(EXIT_FAILURE);
    // }
    // if (setsockopt(sockfd_udp, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0)
    // {
    //     perror("setsockopt failed while setting up membership to udp multicast");
    //     exit(EXIT_FAILURE);
    // }

    // setting up tcp for client side usage (SERVER - MarketFeed)
    int opt = 1;
    int PORT = 8000;
    addr_tcp.sin_family = AF_INET;
    addr_tcp.sin_addr.s_addr  = inet_addr("127.0.0.1"); // put ethernet address here
    addr_tcp.sin_port = htons(PORT);
    addrlen_tcp = sizeof(addr_tcp);

    int status = connect(sockfd_tcp_recv, (struct sockaddr*) &addr_tcp, addrlen_tcp);
    if (status < 0)
    {
        perror("Error in connecting to MarketFeed");
        exit(EXIT_FAILURE);
    }


    int port  = 8080; 
    setsockopt(sockfd_tcp_send, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    servaddr.sin_family = AF_INET; 
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);  // ISSUE change to ethernet
    servaddr.sin_port = htons(port);
    // if (bind(sockfd_tcp, (struct sockaddr *)&addr_tcp, sizeof(addr_tcp)) < 0)
    // {
    //     perror("bind failed for tcp socket");
    //     exit(EXIT_FAILURE);
    // }
    // if (listen(sockfd_tcp, connection_backlog) < 0) // need to handle this error
    // {
    //     perror("listen");
    //     exit(EXIT_FAILURE);
    // }
    if ((bind(sockfd_tcp_send, (struct sockaddr*)&servaddr, sizeof(servaddr))) != 0) { 
        perror("socket bind failed...\n"); 
        exit(0); 
    }
    if ((listen(sockfd_tcp_send, 1)) != 0) { // for rt alone 
        perror("Listen failed...\n"); 
        exit(0); 
    }
    // client_len = sizeof(client);
    // connect_ = accept(sockfd_tcp_send, (struct sockaddr*) &client, &client_len);
    // if (connect_ < 0)
    // {
    //     perror("Error in connecting to retransmitter");
    //     exit(EXIT_FAILURE);
    // }
}

void Retransmitter::storeThread()
{
    int trades{};
    size_t buffered = 0; 
    const size_t max_buffer_size = batch.capacity * MAX_MSG_SIZE;

    while (true) 
    {
        // 1. Receive data into the remaining space in the buffer
        int ret = recv(sockfd_tcp_recv, 
                    batch.tcp_buffer + buffered, 
                    max_buffer_size - buffered, 
                    MSG_DONTWAIT);

        if (ret < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                // Optional: Put thread to sleep briefly, or use epoll to avoid burning CPU
                continue; 
            }
            perror("recv failed");
            break; // Handle fatal error (e.g., reconnect)
        } 
        else if (ret == 0) {
            std::cout << "Peer disconnected gracefully.\n";
            break; 
        }

        buffered += ret;
        size_t offset = 0;

        // 2. Parse as many complete messages as we have
        while (buffered - offset >= sizeof(MarketDataMessage)) 
        {
            auto* msg = (MarketDataMessage*) (batch.tcp_buffer + offset);
            
            // CRITICAL: Ensure `q` is copying the message BY VALUE, not just storing the pointer!
            // If q stores pointers, you must copy the data to a new struct/buffer here.
            std::memcpy(&buffer[msg->seq_num & SIZE - 1], msg, sizeof(MarketDataMessage)); 
            
            offset += sizeof(MarketDataMessage);
            trades++;
        }

        // 3. Shift any leftover partial message to the front of the buffer
        size_t leftover = buffered - offset;
        if (leftover > 0 && offset > 0) {
            std::memmove(batch.tcp_buffer, batch.tcp_buffer + offset, leftover);
        }
        
        // Update buffered to reflect only the leftover bytes
        buffered = leftover; 

        // std::cout << trades << "\n"; // Be careful with I/O in a hot loop
    }
    done.store(true, std::memory_order_release);
}

void Retransmitter::listenerThread()
{
    // 1. Setup the poll list. The first entry is the server listening socket.
    std::vector<struct pollfd> poll_fds;
    poll_fds.push_back({sockfd_tcp_send, POLLIN, 0});

    std::cout << "[RT] Retransmit server active. Monitoring for requests...\n";

    while (true)
    {
        // 2. Wait for activity (100ms timeout to allow checking the 'done' flag)
        int activity = poll(poll_fds.data(), poll_fds.size(), 100);
        bool mfr_finished = done.load(std::memory_order_acquire);
        if (activity < 0) {
            if (errno == EINTR) continue;
            perror("poll error");
            break;
        }

        if (activity == 0 && mfr_finished)
        { 
            break; // Timeout: loop back to check 'done'
        }


        // 3. Handle New Connections (on the server socket)
        if (poll_fds[0].revents & POLLIN)
        {
            struct sockaddr_in client_addr;
            socklen_t client_len = sizeof(client_addr);
            int new_fd = accept(poll_fds[0].fd, (struct sockaddr*)&client_addr, &client_len);

            if (new_fd >= 0) {
                // Set to non-blocking so a slow client doesn't hang the whole loop
                fcntl(new_fd, F_SETFL, O_NONBLOCK);
                poll_fds.push_back({new_fd, POLLIN, 0});
                std::cout << "[RT] Accepted new Listener. Total: " << poll_fds.size() - 1 << "\n";
            }
            if (--activity <= 0) continue;
        }

        // 4. Handle Incoming Requests (from existing clients)
        for (size_t i = 1; i < poll_fds.size(); ++i)
        {
            if (poll_fds[i].revents & POLLIN)
            {
                uint64_t requested_seq = 0;
                // Attempt to read the 8-byte sequence number
                ssize_t n = recv(poll_fds[i].fd, &requested_seq, sizeof(uint64_t), 0);

                if (n == sizeof(uint64_t))
                {
                    // Success! Fetch from the circular buffer using the power-of-2 mask
                    MarketDataMessage* cached = &buffer[requested_seq & (SIZE - 1)];

                    // Safety Check: Verify the cached message actually matches the request
                    if (cached->seq_num == requested_seq)
                    {
                        // Send the 64-byte message back (MSG_NOSIGNAL prevents crash if client drops)
                        send(poll_fds[i].fd, cached, sizeof(MarketDataMessage), MSG_NOSIGNAL);
                    }
                }
                else if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK))
                {
                    // Client disconnected or error
                    std::cout << "[RT] Listener disconnected.\n";
                    close(poll_fds[i].fd);
                    poll_fds.erase(poll_fds.begin() + i);
                    --i; // Adjust index due to removal
                }
                
                if (--activity <= 0) break;
            }
        }
    }

    // Cleanup: Close all active client connections on shutdown
    for (int i = 1; i < poll_fds.size(); i++)
    {
        shutdown(poll_fds[i].fd, SHUT_WR);
        close(poll_fds[i].fd);
    }
    close(poll_fds[0].fd);
}

void Retransmitter::init_batch(int cap)
{
    batch.capacity = cap;
    batch.tcp_buffer = (char*) aligned_alloc(64, cap * MAX_MSG_SIZE);
    
}
}
// #include <thread>
// #include <chrono>
// int main()
// {
//     Retransmitter rt;
//     auto tp = std::chrono::steady_clock::now();
//     std::thread store ([&]{ rt.storeThread(); });
//     std::thread listen ([&]{ rt.listenerThread(); });
//     store.join();
//     listen.join();
//     auto tp2 = std::chrono::steady_clock::now();
//     auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(tp2 - tp);
//     std::cout << duration.count() << "ms";
// }