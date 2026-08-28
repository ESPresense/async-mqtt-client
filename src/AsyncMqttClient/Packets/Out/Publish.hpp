#pragma once

#include <cstring>  // strlen
#include <cstdlib>  // malloc/free
#include <memory>   // std::unique_ptr

#include "OutPacket.hpp"
#include "../../Flags.hpp"
#include "../../Helpers.hpp"
#include "../../Storage.hpp"

namespace AsyncMqttClientInternals {
class PublishOutPacket : public OutPacket {
 public:
  PublishOutPacket(const char* topic, uint8_t qos, bool retain, const char* payload, size_t length);
  const uint8_t* data(size_t index = 0) const;
  size_t size() const;

  bool valid() const { return _data != nullptr; }  // false if the buffer alloc failed

  void setDup();  // you cannot unset dup

 private:
  // Owning buffer rather than std::vector, allocated with malloc.
  //
  // Consumers build with -fno-exceptions AND -fno-unwind-tables, so the linked image
  // contains no .eh_frame at all. That makes a throwing allocation an abort(), and it
  // equally rules out new (std::nothrow): its null return comes from a catch block the
  // unwinder can never reach without unwind data, so that aborts too. malloc is the
  // only allocator here that actually reports failure.
  struct Free { void operator()(uint8_t* p) const { std::free(p); } };
  std::unique_ptr<uint8_t[], Free> _data;
  size_t _size = 0;
};
}  // namespace AsyncMqttClientInternals
