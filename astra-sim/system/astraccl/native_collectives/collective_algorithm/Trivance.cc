/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/system/astraccl/native_collectives/collective_algorithm/Trivance.hh"

#include <iostream>
#include <algorithm>

#include "astra-sim/system/PacketBundle.hh"
#include "astra-sim/system/RecvPacketEventHandlerData.hh"

using namespace AstraSim;

static uint64_t ceil_div_u64(uint64_t a, uint64_t b) {
  return (b == 0) ? a : (a + b - 1) / b;
}

static int ceil_log3(int n) {
  if (n <= 1) return 0;
  int steps = 0;
  long long reach = 1;
  while (reach < n) {
    reach *= 3;
    steps++;
  }
  return steps;
}

Trivance::Trivance(ComType type,
                   int id,
                   RingTopology* ring_topology,
                   uint64_t data_size)
    : Algorithm() {
  this->comType = type;
  this->id = id;
  this->logical_topo = ring_topology;
  this->data_size = data_size;

  this->nodes_in_ring = ring_topology->get_nodes_in_ring();

  if (comType != ComType::All_Reduce) {
    Sys::sys_panic("Trivance: AllReduce only!");
  }

  // Final output size for AllReduce is data size (m)
  this->final_data_size = data_size;

  this->dimension = specify_direction();

  // Follow the pattern of other algorithms
  if (ring_topology->get_dimension() == RingTopology::Dimension::Local) {
    transmition = MemBus::Transmition::Fast;
  } else {
    transmition = MemBus::Transmition::Usual;
  }

  this->total_size = data_size; // m
  this->steps_total = ceil_log3(nodes_in_ring); // log3(n)

  // Important: two neighbors per step
  this->parallel_reduce = 2;

  // Total sends: ReduceScatter 2 per step + AllGather 2 per step
  this->stream_count = 4 * steps_total;

  this->max_count = stream_count;
  this->remained_packets_per_message = 1;
  this->remained_packets_per_max_count = 1;

  this->free_packets = 0;
  this->total_packets_sent = 0;
  this->total_packets_received = 0;

  this->zero_latency_packets = 0;
  this->non_zero_latency_packets = 0;

  this->phase = Phase::ReduceScatter;
  this->step = 0;
  this->distance = 1;
  this->recv_in_step = 0;
  this->dir_toggle = 0;

  this->done = (steps_total == 0);
  this->drain_injected = false;

  // During reduce-scatter we process (reduce) while during allgather we only forward.
  this->processed = true;
  this->send_back = false;
  this->NPU_to_MA = true;

  // Start reduce-scatter with full data size = m
  this->cur_size = total_size;
  this->bundle_size = cur_size;
}

RingTopology::Direction Trivance::specify_direction() {
  return RingTopology::Direction::Clockwise;
}

int Trivance::get_non_zero_latency_packets() {
  return 0;
}

void Trivance::run(EventType event, CallData* data) {
  if (event == EventType::General) {
    free_packets += 1;
    ready();
    iteratable();
    return;
  }

  if (event == EventType::StreamInit) {
    if (done) {
      stream_count = 0;
      iteratable();
      return;
    }

    // Start reduce-scatter step 0 and create two packets for two neighbors separately.
    phase = Phase::ReduceScatter;
    processed = true;

    step = 0;
    distance = 1;
    recv_in_step = 0;
    dir_toggle = 0;
    cur_size = total_size;

    // Create two packets for step 0
    for (int i = 0; i < parallel_reduce; i++) {
      insert_packet(nullptr);
    }
    return;
  }

  if (event == EventType::PacketReceived) {
    total_packets_received++;
    recv_in_step++;

    // Wait for two packets getting both received in this step.
    if (recv_in_step == 2) {
      recv_in_step = 0;
      if (phase == Phase::ReduceScatter) {
        cur_size = std::max<uint64_t>(1, ceil_div_u64(cur_size, 3));
        step++;
        if (step >= steps_total) {
          // Switch to AllGather
          phase = Phase::AllGather;
          processed = false;

          step = 0;
          distance = 1;
          dir_toggle = 0;
          for (int i = 0; i < parallel_reduce; i++) {
            insert_packet(nullptr);
          }
        } else {
          distance *= 3;
          dir_toggle = 0;
          for (int i = 0; i < parallel_reduce; i++) {
            insert_packet(nullptr);
          }
        }
      } else {
        // AllGather phase
        cur_size = std::min<uint64_t>(total_size, cur_size * 3);
        step++;

        // Make sure the process ends cleanly by injecting empty packets 
        // (make the free_packets equal to parallel_reduce)
        if (step >= steps_total) {
          done = true;

          if (!drain_injected) {
            drain_injected = true;

            // Inject two empty packets
            uint64_t saved_cur = cur_size;
            cur_size = 0;
            dir_toggle = 0;
            for (int i = 0; i < parallel_reduce; i++) {
              insert_packet(nullptr);
            }
            cur_size = saved_cur;
          }

          iteratable();
          return;
        } else {
          distance *= 3;
          dir_toggle = 0;
          for (int i = 0; i < parallel_reduce; i++) {
            insert_packet(nullptr);
          }
        }
      }
    }

    iteratable();
    return;
  }
}

void Trivance::release_packets() {
  if (locked_packets.empty()) return;
  if (NPU_to_MA) {
    (new PacketBundle(stream->owner, stream, locked_packets, processed,
                      send_back, bundle_size, transmition))
        ->send_to_MA();
  } else {
    (new PacketBundle(stream->owner, stream, locked_packets, processed,
                      send_back, bundle_size, transmition))
        ->send_to_NPU();
  }

  locked_packets.clear();
}

void Trivance::process_max_count() {
  // Not used.
}

void Trivance::process_stream_count() {
  if (remained_packets_per_message > 0) {
    remained_packets_per_message--;
  }
  if (remained_packets_per_message == 0 && stream_count > 0) {
    stream_count--;
    if (stream_count > 0) remained_packets_per_message = 1;
  }
  if (remained_packets_per_message == 0 && stream_count == 0 &&
      stream->state != StreamState::Dead) {
    stream->changeState(StreamState::Zombie);
  }
}

void Trivance::insert_packet(Callable* sender) {
  // Select partner: left neighbor or right neighbor based on dir_toggle
  int partner;
  if (dir_toggle == 0) {
    partner = (id + (int)distance) % nodes_in_ring;
  } else {
    partner = (id - (int)distance + nodes_in_ring) % nodes_in_ring;
  }
  dir_toggle = (dir_toggle + 1) % 2;
  // Log out the info to debug
  std::cerr
      << "[Trivance] rank=" << id
      << " step=" << step
      << " dist=" << distance
      << " peer=" << partner
      << std::endl;
  uint64_t send_size = 0;

  if (done && drain_injected) {
    send_size = 0;
  } else if (phase == Phase::ReduceScatter) {
    send_size = ceil_div_u64(cur_size, 3);
  } else {
    // AllGather: send everything we have so far
    send_size = cur_size;
  }

  this->bundle_size = send_size;
  // Create packets
  packets.push_back(MyPacket(send_size, stream->current_queue_id, partner, partner));
  packets.back().sender = sender;
  locked_packets.push_back(&packets.back());

  release_packets();
}

bool Trivance::ready() {
  if (stream->state == StreamState::Created || stream->state == StreamState::Ready) {
    stream->changeState(StreamState::Executing);
  }
  if (stream_count == 0) return false;

  if (packets.empty() || free_packets == 0) return false;

  MyPacket packet = packets.front();

  sim_request snd_req;
  snd_req.srcRank = id;
  snd_req.dstRank = packet.preferred_dest;
  snd_req.tag = stream->stream_id;
  snd_req.reqType = UINT8;
  snd_req.vnet = stream->current_queue_id;

  stream->owner->front_end_sim_send(
      0, Sys::dummy_data, packet.msg_size, UINT8, packet.preferred_dest,
      stream->stream_id, &snd_req, Sys::FrontEndSendRecvType::COLLECTIVE,
      &Sys::handleEvent, nullptr);

  sim_request rcv_req;
  rcv_req.vnet = stream->current_queue_id;

  RecvPacketEventHandlerData* ehd = new RecvPacketEventHandlerData(
      stream, stream->owner->id, EventType::PacketReceived,
      packet.preferred_vnet, packet.stream_id);

  stream->owner->front_end_sim_recv(
      0, Sys::dummy_data, packet.msg_size, UINT8, packet.preferred_src,
      stream->stream_id, &rcv_req, Sys::FrontEndSendRecvType::COLLECTIVE,
      &Sys::handleEvent, ehd);

  reduce();
  return true;
}

void Trivance::reduce() {
  process_stream_count();
  packets.pop_front();
  free_packets--;
  total_packets_sent++;
}

bool Trivance::iteratable() {
  if (stream_count == 0 && free_packets == parallel_reduce * 1) {
    exit();
    return false;
  }
  return true;
}

void Trivance::exit() {
  if (!packets.empty()) packets.clear();
  if (!locked_packets.empty()) locked_packets.clear();

  stream->owner->proceed_to_next_vnet_baseline((StreamBaseline*)stream);
}