// Order receiving server for OMS.
// Accepts TCP connections, creates one session thread per client,
// and forwards received client orders into the OMS queue.

#include "oms_order_receiving_server.h"

#include <cerrno>
#include <algorithm>
#include <cstring>
#include <iostream>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace oms {

OrderReceivingServer::OrderReceivingServer(OrderManagementSystem& oms_ref, uint16_t port_value)
	: oms(oms_ref), port(port_value) {}

OrderReceivingServer::~OrderReceivingServer() {
	stop();
}

void OrderReceivingServer::start() {
	accept_thread = std::thread(&OrderReceivingServer::acceptLoop, this);
}

void OrderReceivingServer::stop() {
	shutdown.store(true, std::memory_order_release);

	if (server_fd != -1) {
		::shutdown(server_fd, SHUT_RDWR);
		::close(server_fd);
		server_fd = -1;
	}

	if (accept_thread.joinable()) {
		accept_thread.join();
	}

	{
		std::lock_guard<std::mutex> lock(client_mutex);
		for (int client_fd : active_client_fds) {
			::shutdown(client_fd, SHUT_RDWR);
			::close(client_fd);
		}
		active_client_fds.clear();
	}

	for (auto& thread : client_threads) {
		if (thread.joinable()) {
			thread.join();
		}
	}
	client_threads.clear();
}

void OrderReceivingServer::acceptLoop() {
	server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
	if (server_fd < 0) {
		std::cerr << "OMS order receiving server socket creation failed: " << std::strerror(errno) << '\n';
		return;
	}

	int opt = 1;
	if (::setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
		std::cerr << "OMS order receiving server setsockopt failed: " << std::strerror(errno) << '\n';
	}

	sockaddr_in addr{};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = htons(port);

	if (::bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
		std::cerr << "OMS order receiving server bind failed on port " << port << ": " << std::strerror(errno) << '\n';
		::close(server_fd);
		server_fd = -1;
		return;
	}

	if (::listen(server_fd, 64) < 0) {
		std::cerr << "OMS order receiving server listen failed: " << std::strerror(errno) << '\n';
		::close(server_fd);
		server_fd = -1;
		return;
	}

	while (!shutdown.load(std::memory_order_acquire)) {
		int client_fd = ::accept(server_fd, nullptr, nullptr);
		if (client_fd < 0) {
			if (shutdown.load(std::memory_order_acquire)) {
				break;
			}

			if (errno == EINTR) {
				continue;
			}

			std::cerr << "OMS order receiving server accept failed: " << std::strerror(errno) << '\n';
			continue;
		}

		{
			std::lock_guard<std::mutex> lock(client_mutex);
			active_client_fds.push_back(client_fd);
			client_threads.emplace_back(&OrderReceivingServer::clientSession, this, client_fd);
		}
	}
}

void OrderReceivingServer::clientSession(int client_fd) {
	while (!shutdown.load(std::memory_order_acquire)) {
		oms::ClientOrder order{};
		if (!readExact(client_fd, &order, sizeof(order))) {
			break;
		}

		bool enqueued = false;
		while (!shutdown.load(std::memory_order_acquire) && !enqueued) {
			{
				std::lock_guard<std::mutex> lock(enqueue_mutex);
				enqueued = oms.enqueueClientOrder(order);
			}

			if (!enqueued) {
				std::this_thread::yield();
			}
		}
	}

	::shutdown(client_fd, SHUT_RDWR);
	::close(client_fd);
	removeClientFd(client_fd);
}

bool OrderReceivingServer::readExact(int client_fd, void* buffer, std::size_t size) {
	auto* cursor = static_cast<char*>(buffer);
	std::size_t received = 0;

	while (received < size && !shutdown.load(std::memory_order_acquire)) {
		ssize_t bytes = ::recv(client_fd, cursor + received, size - received, 0);
		if (bytes == 0) {
			return false;
		}
		if (bytes < 0) {
			if (errno == EINTR) {
				continue;
			}
			std::cerr << "OMS order receiving server recv failed: " << std::strerror(errno) << '\n';
			return false;
		}

		received += static_cast<std::size_t>(bytes);
	}

	return received == size;
}

void OrderReceivingServer::removeClientFd(int client_fd) {
	std::lock_guard<std::mutex> lock(client_mutex);
	auto it = std::find(active_client_fds.begin(), active_client_fds.end(), client_fd);
	if (it != active_client_fds.end()) {
		active_client_fds.erase(it);
	}
}

}
