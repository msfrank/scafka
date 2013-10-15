package com.syntaxjockey.scafka

import akka.util.{ByteString, ByteStringBuilder}
import scala.concurrent.duration.Duration

sealed trait ProtocolRequest[T] extends Payload with PayloadWriter {
  val api: ApiKey.ApiKey
  def readResponse(data: ByteString): T
}

/**
 *
 * @param topics
 */
case class MetadataRequest(topics: Seq[String]) extends ProtocolRequest[MetadataResponse] {
  
  val api = ApiKey.MetadataRequest

  def readResponse(data: ByteString) = MetadataResponse(data)
  
  def writePayload(output: ByteStringBuilder) {
    output.putInt(topics.length)
    for (topic <- topics) {
      output.putShort(topic.length)
      output.putBytes(topic.getBytes(charset))
    }
  }

  def payloadSize: Int = 4 + topics.foldLeft(0){ case (acc, topic) => acc + topic.length }
}

/**
 *
 * @param topics
 * @param timeout
 * @param requiredAcks
 */
case class ProduceRequest(topics: Map[String,Map[Int,KafkaMessage]], timeout: Duration, requiredAcks: Int) extends ProtocolRequest[ProduceResponse] {

  val api = ApiKey.ProduceRequest

  def readResponse(data: ByteString) = ProduceResponse(data)
  
  def writePayload(output: ByteStringBuilder) {
    output.putShort(requiredAcks)
    output.putInt(timeout.toMillis.toInt)
    output.putInt(topics.size)
    topics.foreach { case (topic, partitions) =>
      output.putShort(topic.length)
      output.putBytes(topic.getBytes(charset))
      output.putInt(partitions.size)
      partitions.foreach { case (partition, message) =>
        output.putInt(partition)
        val messageSize = message.payloadSize
        output.putInt(12 + messageSize)
        output.putLong(0) // offset
        output.putInt(messageSize)
        message.writePayload(output)
      }
    }
  }

  def payloadSize: Int = {
    10 + topics.foldLeft(0) { case (acc1, (topic, partitions)) =>
      acc1 + 6 + topic.length + partitions.foldLeft(0) { case (acc2, (partition, message)) =>
        acc2 + 20 + message.payloadSize
      }
    }
  }
}

sealed trait ProtocolResponse extends Payload

/**
 *
 */
case class MetadataResponse() extends ProtocolResponse

object MetadataResponse {
  def apply(data: ByteString): MetadataResponse = {
    MetadataResponse()
  }
}

/**
 *
 * @param responses
 */
case class ProduceResponse(responses: Map[String, Map[Int,(ErrorCode.ErrorCode,Long)]]) extends ProtocolResponse

object ProduceResponse extends Payload {
  def apply(data: ByteString): ProduceResponse = {
    val iterator = data.iterator
    val numTopics = iterator.getInt
    val responses: Map[String,Map[Int,(ErrorCode.ErrorCode,Long)]] =  (0 until numTopics).map { _ =>
      val topicLen = iterator.getShort
      val topic = new Array[Byte](topicLen)
      iterator.getBytes(topic)
      val numPartitions = iterator.getInt
      val partitions = (0 until numPartitions).map { _ =>
        val partition = iterator.getInt
        val errorCode = ErrorCode(iterator.getShort)
        val offset = iterator.getLong
        partition -> (errorCode,offset)
      }.toMap
      new String(topic) -> partitions
    }.toMap
    ProduceResponse(responses)
  }
}