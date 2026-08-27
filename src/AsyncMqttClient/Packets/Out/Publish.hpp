#pragma once

#include <cstring>  // strlen
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

  bool valid() const { return _data != nullptr; }  // false if the (nothrow) buffer alloc failed

  void setDup();  // you cannot unset dup

 private:
  // Owning buffer rather than std::vector: consumers build with -fno-exceptions, where a
  // failed vector reserve/insert is an abort() rather than a failed publish. Allocated
  // nothrow, so a null buffer is reported via valid() and publish() drops the message.
  std::unique_ptr<uint8_t[]> _data;
  size_t _size = 0;
};
}  // namespace AsyncMqttClientInternals
