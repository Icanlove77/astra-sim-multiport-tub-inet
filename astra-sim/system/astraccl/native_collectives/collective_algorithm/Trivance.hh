/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __TRIVANCE_HH__
#define __TRIVANCE_HH__

#include <list>
#include <cstdint>

#include "astra-sim/system/MemBus.hh"
#include "astra-sim/system/MyPacket.hh"
#include "astra-sim/system/astraccl/Algorithm.hh"
#include "astra-sim/system/astraccl/native_collectives/logical_topology/RingTopology.hh"

namespace AstraSim {

class Trivance : public Algorithm {
  public:
    Trivance(ComType type,
             int id,
             RingTopology* ring_topology,
             uint64_t data_size);

    virtual void run(EventType event, CallData* data);

    RingTopology::Direction specify_direction();
    void process_stream_count();
    void release_packets();
    virtual void process_max_count();
    void reduce();
    bool iteratable();
    virtual int get_non_zero_latency_packets();
    void insert_packet(Callable* sender);
    bool ready();
    void exit();

    // Common fields
    RingTopology::Direction dimension;
    MemBus::Transmition transmition;

    int zero_latency_packets;
    int non_zero_latency_packets;

    int id;
    int nodes_in_ring;

    int stream_count;  
    int max_count;
    int remained_packets_per_max_count;
    int remained_packets_per_message;

    int parallel_reduce;  // Controls how many packets one node can send in parallel

    std::list<MyPacket> packets;
    std::list<MyPacket*> locked_packets;

    long free_packets;
    long total_packets_sent;
    long total_packets_received;

    bool processed;
    bool send_back;
    bool NPU_to_MA;

    // Fields for Trivance
    enum class Phase { ReduceScatter, AllGather };

    Phase phase;
    int steps_total;       // log3(n)
    int step;              // 0..steps_total-1
    uint64_t distance;     // For example: 1,3,9,...

    int recv_in_step;      // Counter for packets received in current step (Finished when reaches 2)
    int dir_toggle;        // Controls the direction of current step

    bool done;
    bool drain_injected;   // Make sure the process ends cleanly

    uint64_t total_size;   // m (bytes)
    uint64_t cur_size;     // Current size being processed
    uint64_t bundle_size;  // size passed to PacketBundle (1 packet per bundle)
};

}  // namespace AstraSim

#endif /* __TRIVANCE_HH__ */