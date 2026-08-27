#include "Publish.hpp"

#include <cstring>  // memcpy
#include <new>      // std::nothrow

using AsyncMqttClientInternals::PublishOutPacket;

PublishOutPacket::PublishOutPacket(const char* topic, uint8_t qos, bool retain, const char* payload, size_t length) {
  char fixedHeader[5];
  fixedHeader[0] = AsyncMqttClientInternals::PacketType.PUBLISH;
  fixedHeader[0] = fixedHeader[0] << 4;
  // if (dup) fixedHeader[0] |= AsyncMqttClientInternals::HeaderFlag.PUBLISH_DUP;
  if (retain) fixedHeader[0] |= AsyncMqttClientInternals::HeaderFlag.PUBLISH_RETAIN;
  switch (qos) {
    case 0:
      fixedHeader[0] |= AsyncMqttClientInternals::HeaderFlag.PUBLISH_QOS0;
      break;
    case 1:
      fixedHeader[0] |= AsyncMqttClientInternals::HeaderFlag.PUBLISH_QOS1;
      break;
    case 2:
      fixedHeader[0] |= AsyncMqttClientInternals::HeaderFlag.PUBLISH_QOS2;
      break;
  }

  uint16_t topicLength = strlen(topic);
  char topicLengthBytes[2];
  topicLengthBytes[0] = topicLength >> 8;
  topicLengthBytes[1] = topicLength & 0xFF;

  uint32_t payloadLength = length;
  if (payload != nullptr && payloadLength == 0) payloadLength = strlen(payload);

  uint32_t remainingLength = 2 + topicLength + payloadLength;
  if (qos != 0) remainingLength += 2;
  uint8_t remainingLengthLength = AsyncMqttClientInternals::Helpers::encodeRemainingLength(remainingLength, fixedHeader + 1);

  size_t neededSpace = 0;
  neededSpace += 1 + remainingLengthLength;
  neededSpace += 2;
  neededSpace += topicLength;
  if (qos != 0) neededSpace += 2;
  if (payload != nullptr) neededSpace += payloadLength;

  // Nothrow allocation: a failure leaves _data == nullptr / _size == 0 (valid() == false), and
  // AsyncMqttClient::publish() drops the message instead of aborting under memory pressure.
  _data.reset(new (std::nothrow) uint8_t[neededSpace]);
  if (!_data) return;

  _packetId = (qos != 0) ? _getNextPacketId() : 1;
  char packetIdBytes[2];
  packetIdBytes[0] = _packetId >> 8;
  packetIdBytes[1] = _packetId & 0xFF;

  size_t o = 0;
  memcpy(_data.get() + o, fixedHeader, 1 + remainingLengthLength); o += 1 + remainingLengthLength;
  memcpy(_data.get() + o, topicLengthBytes, 2); o += 2;
  memcpy(_data.get() + o, topic, topicLength); o += topicLength;
  if (qos != 0) {
    memcpy(_data.get() + o, packetIdBytes, 2); o += 2;
    _released = false;
  }
  if (payload != nullptr && payloadLength > 0) {
    memcpy(_data.get() + o, payload, payloadLength); o += payloadLength;
  }
  _size = o;
}

const uint8_t* PublishOutPacket::data(size_t index) const {
  return _data.get() + index;
}

size_t PublishOutPacket::size() const {
  return _size;
}

void PublishOutPacket::setDup() {
  if (_data) _data[0] |= AsyncMqttClientInternals::HeaderFlag.PUBLISH_DUP;
}
