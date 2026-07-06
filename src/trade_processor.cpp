#include "trade_processor.h"




namespace TradeProcessor{



    spsc_queue::spsc_queue(std::array<int, mem_regions>& fdArray):
    fdArray(fdArray)
    {
        head.store(0, std::memory_order_relaxed);
        tail.store(0, std::memory_order_relaxed);

        
    }


    spsc_queue::spsc_queue(std::string file_name_base, std::array<int, mem_regions>& fdArray) :
    file_name_base(file_name_base), 
    fdArray(fdArray)
    {
        head.store(0, std::memory_order_relaxed);
        tail.store(0, std::memory_order_relaxed);
        for (int i = 0; i < mem_regions - 1; i++)
        {
            std::string file_name = path + file_name_base + std::to_string(fileNumber++);
            int fd = open(file_name.data(), O_CREAT | O_RDWR, 0644);
            if (fd == -1)
            {
                perror("Open failed for file");
                std::cout << i << "\n";
                exit(-1);
            }
            fdArray[i] = fd;
            ftruncate(fd, fileSize);//increase file size
            uint8_t* mmap_ptr = static_cast<uint8_t *>(mmap(nullptr, fileSize, PROT_WRITE, MAP_SHARED, fd, 0));
            if (mmap_ptr == MAP_FAILED)
            {
                perror("Error in initialization");
                exit(-1);
                // error in mmap, based on errno, handle
            }
            madvise(mmap_ptr, fileSize, MADV_WILLNEED);
            give_region(mmap_ptr);
        }
    }
    bool spsc_queue::give_region(uint8_t* pmem_region)
    {
        uint64_t _head = head.load(std::memory_order_acquire);
        uint64_t _tail = tail.load(std::memory_order_relaxed);

        if (((_tail + 1) & (mem_regions - 1)) != (_head & (mem_regions - 1))) // queue not full
        {
            memRegions[_tail & (mem_regions - 1)] = pmem_region;
            tail.store(_tail + 1, std::memory_order_release);
            return true;
        }

        return false;
    }

    bool spsc_queue::get_region(uint8_t*& out)
    {
        uint64_t _head = head.load(std::memory_order_relaxed);
        uint64_t _tail = tail.load(std::memory_order_acquire);

        if (_tail != _head) // queue not empty
        {
            out = memRegions[_head & (mem_regions - 1)];
            head.store(_head + 1, std::memory_order_release);
            return true;
        }

        return false;
    }
    void TradeProcessor::writerThread(){
        //no shutdown implemented yet. 
        uint64_t writeOffset{0};
        auto tstart = std::chrono::steady_clock::now();
        uint8_t* mem_region = nullptr;

        int trade_counter{};
        int region_claims{};
        int region_gaves{};
        while(trade_counter < maxTradesPerTP){ //add in proper 
            while(!trBuffer.any_new_trade()){ // potential fix and backoff?

            }
            if (mem_region == nullptr)
            {
                if (rb_write.get_region(mem_region))
                {
                    writeOffset = 0;
                }
                else
                {

                    continue;
                }
            }
            //write into our memory block

            trBuffer.get_trade(mem_region + writeOffset);
            writeOffset += sizeof(matching_engine::Trade);
            trade_counter++;

            //give memory block to make persisten
            if(writeOffset >= fileSize){
                rb_persist.give_region(mem_region);
                mem_region = nullptr;
            }
        }
        if (mem_region != nullptr){
            rb_persist.give_region(mem_region);
        }
        curr_chunk++;
        t2done.store(true, std::memory_order_release);
        auto tdone = std::chrono::steady_clock::now();
        writerDuration = std::chrono::duration_cast<std::chrono::microseconds>(tdone - tstart).count();

    }

    void TradeProcessor::persistenceThread(){
        auto tstart = std::chrono::steady_clock::now();

        uint8_t* mem_region = nullptr;
        int region_claims{};
        int region_gaves{};
        while(!t2done.load(std::memory_order_acquire)){
            if(mem_region == nullptr) // want potential backoffs, if necessary?
            {
                if (!rb_persist.get_region(mem_region)) // claim new region for persistence
                {
                    continue;
                }
            }

            msync(mem_region, fileSize, MS_SYNC); //want to change this to MS_SYNC
            rb_write.give_region(mem_region); // give back region for writing

            mem_region = nullptr;

        }


        if (mem_region != nullptr){
            msync(mem_region, fileSize, MS_SYNC);
            rb_write.give_region(mem_region);
        }

        //sync remaining regions
        while (rb_persist.get_region(mem_region))
        {
            rb_write.give_region(mem_region); //for easy closure later
            msync(mem_region, fileSize, MS_SYNC);

        }

        //this should be handled while closing the ring buffers add something to close the file descriptors as well
        munmap(mmapPtr, fileSize);
        
        auto tend = std::chrono::steady_clock::now();
        persistenceDuration = std::chrono::duration_cast<std::chrono::microseconds>(tend - tstart).count();
    }

}

// int main(){
//     std::vector<TradeRingBuffer::trade_ring_buffer*> arr_trb;
//     std::vector<TradeProcessor::TradeProcessor*> arr_tp;

//     for (int i = 0; i < TradeRingBuffer::total_ring_buffer_count; i++)
//     {
//         std::string file_name = "file_" + std::to_string(i) + "_";
//         arr_tp.emplace_back(new TradeProcessor::TradeProcessor(file_name, i));
//         arr_trb.emplace_back(new TradeRingBuffer::trade_ring_buffer(true, i));
//     }
//     auto tp_start = std::chrono::steady_clock::now();
//     int trade_count{1};
//     for(int i = 0; i < TradeProcessor::maxTradesPerTP * TradeRingBuffer::total_ring_buffer_count; i++){
//         matching_engine::Trade* t = new matching_engine::Trade();
//         t->trade_id = trade_count++;
//         arr_trb[i / (TradeProcessor::maxTradesPerTP)]->add_trade(*t);
//     }
//     auto tp_end = std::chrono::steady_clock::now();
//     std::cout << "To write: " << static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(tp_end - tp_start).count() )/ (1'000'000)<<" s\n";
//     auto tp1 = std::chrono::steady_clock::now();
//     std::vector<std::thread> writer_thread_pool;
//     std::vector<std::thread> persistence_thread_pool;
//     for (int i = 0; i < TradeRingBuffer::total_ring_buffer_count; i++)
//     {   
//         writer_thread_pool.emplace_back( [&, i]{ arr_tp[i]->writerThread(); });
//         persistence_thread_pool.emplace_back([&, i]{ arr_tp[i]->persistenceThread(); });
//     }

//     marketOpen.store(false, std::memory_order_release);
//     for (int i = 0; i < TradeRingBuffer::total_ring_buffer_count; i++)
//     {
//         writer_thread_pool[i].join();
//         persistence_thread_pool[i].join();
//     }
//     // t4.join();
//     auto tp2 = std::chrono::steady_clock::now();
//     std::cout<<"Time taken: "<<std::chrono::duration_cast<std::chrono::microseconds>(tp2 - tp1).count()<<" mus\n";
//     std::cout<<"Time spent in writer thread: "<<writerDuration<<" mus \n";
//     std::cout<<"Time spent in persistence thread: "<<persistenceDuration<<" mus \n";
//     std::cout<<"Average time per trade: "<<static_cast<double>(persistenceDuration)/(TradeProcessor::maxTradesPerTP * TradeRingBuffer::total_ring_buffer_count)<<" mus\n";

// }