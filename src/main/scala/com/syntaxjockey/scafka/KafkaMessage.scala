package com.syntaxjockey.scafka

import akka.util.ByteStringBuilder
import java.nio.ByteOrder
import java.nio.charset.Charset
import java.util.zip.CRC32

/**
 *
 */
abstract class Payload {
  val charset = Charset.forName("UTF-8")                              // FIXME: is this the correct string encoding?
  implicit val byteOrder: ByteOrder = java.nio.ByteOrder.BIG_ENDIAN   // ensure we are using big-endian on the wire
}

trait PayloadWriter {
  def writePayload(output: ByteStringBuilder)
  def payloadSize: Int
}

/**
 *
 * @param request
 * @param apiVersion
 * @param correlationId
 * @param clientId
 */
case class KafkaRequest(request: ProtocolRequest[_], apiVersion: Int, correlationId: Int, clientId: String) extends Payload with PayloadWriter {

  def writePayload(output: ByteStringBuilder) {
    output.putShort(request.api.id)
    output.putShort(apiVersion)
    output.putInt(correlationId)
    output.putShort(clientId.length)
    output.putBytes(clientId.getBytes(charset))
    request.writePayload(output)
  }

  def payloadSize: Int = 10 + clientId.length +  request.payloadSize
}

object KafkaRequest {
  val VERSION = 0
}

/**
 *
 * @param key
 * @param value
 * @param attributes
 * @param magic
 */
case class KafkaMessage(key: Option[Array[Byte]], value: Option[Array[Byte]], attributes: Byte, magic: Byte) extends Payload with PayloadWriter {
  def writePayload(output: ByteStringBuilder) {
    val bytes = new ByteStringBuilder()
    bytes.putByte(magic)
    bytes.putByte(attributes)
    key match {
      case Some(array) =>
        bytes.putInt(array.length)
        bytes.putBytes(array)
      case None =>
        bytes.putInt(-1)
    }
    value match {
      case Some(array) =>
        bytes.putInt(array.length)
        bytes.putBytes(array)
      case None =>
        bytes.putInt(-1)
    }
    val crc = new CRC32()
    crc.update(bytes.result().toArray)
    output.putInt(crc.getValue.toInt)
    output.append(bytes.result())
  }
  def payloadSize: Int = {
    val keylen = if (key.isDefined) 4 + key.get.length else 4
    val valuelen = if (value.isDefined) 4 + value.get.length else 4
    6 + keylen + valuelen
  }
}

object KafkaMessage {
  val MAGIC: Byte = 0
}

case class KafkaMessageHeader(message: KafkaMessage, offset: Option[Long] = None)

object ApiKey extends Enumeration {
  type ApiKey = Value
  val ProduceRequest, FetchRequest, OffsetRequest, MetadataRequest, LeaderAndIsrRequest, StopReplicaRequest, OffsetCommitRequest, OffsetFetchRequest = Value
}

object Attributes {
  val CODEC_NONE: Byte = 0
  val CODEC_GZIP: Byte = 1
  val CODEC_SNAPPY: Byte = 2
}

object ErrorCode extends Enumeration {
  type ErrorCode = Value
  val NoError	                    = Value(0)   // No error--it worked!
  val Unknown	                    = Value(-1)  // An unexpected server error
  val OffsetOutOfRange            = Value(1)	 // The requested offset is outside the range of offsets maintained by the server for the given topic/partition.
  val InvalidMessage              = Value(2)	 // This indicates that a message contents does not match its CRC
  val UnknownTopicOrPartition     = Value(3)	 // This request is for a topic or partition that does not exist on this broker.
  val InvalidMessageSize          = Value(4)	 // The message has a negative size
  val LeaderNotAvailable          = Value(5)	 // This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.
  val NotLeaderForPartition       = Value(6)	 // This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.
  val RequestTimedOut             = Value(7)	 // This error is thrown if the request exceeds the user-specified time limit in the request.
  val BrokerNotAvailable          = Value(8)	 // This is not a client facing error and is used only internally by intra-cluster broker communication.
  val ReplicaNotAvailable         = Value(9)	 // What is the difference between this and LeaderNotAvailable?
  val MessageSizeTooLarge         = Value(10)	 // The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.
  val StaleControllerEpochCode    = Value(11)	 // ???
  val OffsetMetadataTooLargeCode  = Value(12)	 // If you specify a string larger than configured maximum for offset metadata
}

