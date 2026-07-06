/* This is not robust yet, as we're dealing with hand off issues with the block. Refer to writer thread for more details.*/
#ifndef TRADE_PROCESSOR
#define TRADE_PROCESSOR
#include <string>
#include <string_view>
#include <cstring>
#include <array>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <atomic>
#include <thread>
#include <chrono>
#include "trade_ring_buffer.h"
#include <array>
#include <utility>
#include <cstdint>

#include "trade.h"
#include "trade_ring_buffer.h"


inline std::atomic<bool> marketOpen{true};
static int writerDuration;
static int persistenceDuration;


namespace TradeProcessor{
    constexpr size_t queue_buffer_size{1 << 20}; //arbitrary size
    const size_t mem_regions(32);
    static int fileNumber{1};
    constexpr static int max_trades_in_a_day{100'000};
    constexpr static int maxTradesPerTP{max_trades_in_a_day / TradeRingBuffer::total_ring_buffer_count}; //change later this constant feels kinda wonky and disconnected with trb count (look at usage everywhere)
    constexpr static int64_t fileSize{maxTradesPerTP * sizeof(matching_engine::Trade) / mem_regions}; //Calculating the max file size, depending that all trades never exceed this amount
    const std::string path{"./file_output/"};


    class spsc_queue{
        private:
            std::array<uint8_t*, mem_regions> memRegions;
            std::array<int, mem_regions> fdArray;
            std::string file_name_base;
            alignas(64) std::atomic_uint64_t head;
            alignas(64) std::atomic_uint64_t tail;

        public:
            spsc_queue(std::array<int, mem_regions>&);
            spsc_queue(std::string, std::array<int, mem_regions>&);
            bool get_region(uint8_t*&); //get a region of memory to consume 
            bool give_region(uint8_t*); //return a region of memory to be consumed
    };
    class TradeProcessor{
    private:
        std::atomic_uint32_t curr_chunk{0};
        uint8_t* mmapPtr;
        uint8_t* mmapPtrTemp;
        std::string fileName;
        uint64_t lastOffset{0};
        int fd;
        std::atomic_bool t2done{false};
        TradeRingBuffer::trade_ring_buffer trBuffer;
        spsc_queue rb_write;
        spsc_queue rb_persist;
        std::array<int, mem_regions> fdArray{};

    public:
        TradeProcessor(std::string fileName_, int id):
        fileName(fileName_),
        trBuffer(false, id),
        rb_write(fileName_, fdArray),
        rb_persist(fdArray)
        {
            


            // //fileName must follow our convention, specified by reallocator
            // fd = open((path + fileName).data(), O_CREAT|O_RDWR, 0644);
            // if(fd == -1){
            //     //error in opening file
            // }
            // ftruncate(fd, fileSize);//increase file size
            // mmapPtr = static_cast<uint8_t*> (mmap(nullptr, fileSize, PROT_WRITE, MAP_SHARED, fd, 0));
            // madvise(mmapPtr, fileSize, MADV_WILLNEED);
            
        }

        void writerThread();
        /*Only has an issue with losing previous block before all chunks could be synced.*/
        void persistenceThread();
    };

}
#endif