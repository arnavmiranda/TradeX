#include "MarketFeedReader.h"
#include <thread>
#include <atomic>
#include <iostream>

int main() {
    std::atomic_bool running{true};
    std::cout << "Starting Live Market Feed Reader..." << std::endl;
    
    // Initialize MFR. Internally it attaches to TradeRingBuffers as Consumer (false)
    MFR::MarketFeedReader mfr(running);

    std::thread reader([&]{ mfr.readThread(); });
    std::thread sender([&]{ mfr.sendThread(); });

    std::cout << "[MFR] Listening for trades and broadcasting UDP to 239.1.1.1:5000" << std::endl;

    reader.join();
    sender.join();
    return 0;
}