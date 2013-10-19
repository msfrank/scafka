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

/**
 *
 * @param replica
 * @param maxWait
 * @param minBytes
 * @param topics
 */
case class FetchRequest(replica: Int, maxWait: Duration, minBytes: Int, topics: Map[String,Seq[FetchPartition]]) extends ProtocolRequest[FetchResponse] {

  val api = ApiKey.FetchRequest

  def readResponse(data: ByteString) = FetchResponse(data)

  def writePayload(output: ByteStringBuilder) {
    output.putInt(replica)
    output.putInt(maxWait.toMillis.toInt)
    output.putInt(minBytes)
    output.putInt(topics.size)
    topics.foreach { case (topic, partitions) =>
      output.putShort(topic.length)
      output.putBytes(topic.getBytes(charset))
      output.putInt(partitions.length)
      partitions.foreach { case partition =>
        output.putInt(partition.partition)
        output.putLong(partition.offset)
        output.putInt(partition.maxBytes)
      }
    }
  }

  def payloadSize: Int = {
    16 + topics.foldLeft(0) { case (acc, (topic, partitions)) =>
      acc + 6 + topic.length + (16 * partitions.length)
    }
  }
}

object FetchRequest {
  val REPLICA_UNSPECIFIED = -1
}

case class FetchPartition(partition: Int, offset: Long, maxBytes: Int)

/**
 *
 * @param replica
 * @param topics
 */
case class OffsetRequest(replica: Int, topics: Map[String,Map[Int,RequestOffsets]]) extends ProtocolRequest[OffsetResponse] {

  val api = ApiKey.OffsetRequest

  def readResponse(data: ByteString) = OffsetResponse(data)

  def writePayload(output: ByteStringBuilder) {
    output.putInt(replica)
    output.putInt(topics.size)
    topics.foreach { case (topic, partitions) =>
      output.putShort(topic.length)
      output.putBytes(topic.getBytes(charset))
      output.putInt(partitions.size)
      partitions.foreach { case (partition, offset) =>
        output.putInt(partition)
        output.putLong(offset.timestamp)
        output.putInt(offset.maxOffsets)
      }
    }
  }

  def payloadSize: Int = {
    8 + topics.foldLeft(0) { case (acc, (topic, offsets)) =>
      acc + 6 + topic.length + (16 * offsets.size)
    }
  }
}

object OffsetRequest {
  val LATEST_OFFSETS = -1.toLong
  val EARLIEST_OFFSETS = -2.toLong
}

case class RequestOffsets(partition: Int, timestamp: Long, maxOffsets: Int)

/**
 *
 */
sealed trait ProtocolResponse extends Payload

/**
 *
 */
case class MetadataResponse(brokers: Map[Int,KafkaBroker], topics: Map[String,TopicMetadata]) extends ProtocolResponse

object MetadataResponse extends Payload {
  def apply(data: ByteString): MetadataResponse = {
    val iterator = data.iterator
    val numBrokers = iterator.getInt
    val brokers: Map[Int,KafkaBroker] = (0 until numBrokers).map { _ =>
      val node = iterator.getInt
      val hostLen = iterator.getShort
      val host = new Array[Byte](hostLen)
      iterator.getBytes(host)
      val port = iterator.getInt
      node -> KafkaBroker(node, new String(host, charset), port)
    }.toMap
    val numTopics = iterator.getInt
    val topics: Map[String,TopicMetadata] = (0 until numTopics).map { _ =>
      val topicError = ErrorCode(iterator.getShort)
      val topicLen = iterator.getShort
      val topicBytes = new Array[Byte](topicLen)
      iterator.getBytes(topicBytes)
      val numPartitions = iterator.getInt
      val partitions: Map[Int,PartitionMetadata] = (0 until numPartitions).map { _ =>
        val partitionError = ErrorCode(iterator.getShort)
        val partition = iterator.getInt
        val leader = iterator.getInt
        val numReplicas = iterator.getInt
        val replicas: Seq[Int] = (0 until numReplicas).map(_ => iterator.getInt)
        val numIsrs = iterator.getInt
        val isrs: Seq[Int] = (0 until numIsrs).map(_ => iterator.getInt)
        partition -> PartitionMetadata(partition, partitionError, leader, replicas, isrs)
      }.toMap
      val topic = new String(topicBytes, charset)
      topic -> TopicMetadata(topic, topicError, partitions)
    }.toMap
    MetadataResponse(brokers, topics)
  }
}

case class PartitionMetadata(partition: Int, errorCode: ErrorCode.ErrorCode, leader: Int, replicas: Seq[Int], isrs: Seq[Int])
case class TopicMetadata(name: String, errorCode: ErrorCode.ErrorCode, partitions: Map[Int,PartitionMetadata])

/**
 *
 * @param responses
 */
case class ProduceResponse(responses: Map[String,Map[Int,ProduceResponsePartition]]) extends ProtocolResponse

object ProduceResponse extends Payload {
  def apply(data: ByteString): ProduceResponse = {
    val iterator = data.iterator
    val numTopics = iterator.getInt
    val responses: Map[String,Map[Int,ProduceResponsePartition]] = (0 until numTopics).map { _ =>
      val topicLen = iterator.getShort
      val topic = new Array[Byte](topicLen)
      iterator.getBytes(topic)
      val numPartitions = iterator.getInt
      val partitions = (0 until numPartitions).map { _ =>
        val partition = iterator.getInt
        val errorCode = ErrorCode(iterator.getShort)
        val offset = iterator.getLong
        partition -> ProduceResponsePartition(partition, errorCode, offset)
      }.toMap
      new String(topic) -> partitions
    }.toMap
    ProduceResponse(responses)
  }
}

case class ProduceResponsePartition(partition: Int, errorCode: ErrorCode.ErrorCode, offset: Long)

/**
 *
 * @param responses
 */
case class FetchResponse(responses: Map[String,Map[Int,FetchResponsePartition]]) extends ProtocolResponse

object FetchResponse extends Payload {
  def apply(data: ByteString): FetchResponse = {
    val iterator = data.iterator
    val numTopics = iterator.getInt
    val responses: Map[String,Map[Int,FetchResponsePartition]] = (0 until numTopics).map { _ =>
      val topicLen = iterator.getShort
      val topic = new Array[Byte](topicLen)
      iterator.getBytes(topic)
      val numPartitions = iterator.getInt
      val partitions = (0 until numPartitions).map { _ =>
        val partition = iterator.getInt
        val errorCode = ErrorCode(iterator.getShort)
        val hiOffset = iterator.getLong
        val messageSetSize = iterator.getInt
        var messageSet = Seq.empty[KafkaMessageHeader]
        while (iterator.hasNext) {
          val offset = iterator.getLong
          val messageSize = iterator.getInt
          val crc = iterator.getInt
          val magic = iterator.getByte
          val attributes = iterator.getByte
          val keyLen = iterator.getInt
          val key: Option[Array[Byte]] = if (keyLen == -1) None else {
            val keyBytes = new Array[Byte](keyLen)
            iterator.getBytes(keyBytes)
            Some(keyBytes)
          }
          val valueLen = iterator.getInt
          val value: Option[Array[Byte]] = if (valueLen == -1) None else {
            val valueBytes = new Array[Byte](valueLen)
            iterator.getBytes(valueBytes)
            Some(valueBytes)
          }
          messageSet = messageSet :+ KafkaMessageHeader(KafkaMessage(key, value, attributes, magic), Some(offset))
        }
        partition -> FetchResponsePartition(partition, errorCode, hiOffset, messageSet)
      }.toMap
      new String(topic, charset) -> partitions
    }.toMap
    FetchResponse(responses)
  }
}

case class FetchResponsePartition(partition: Int, errorCode: ErrorCode.ErrorCode, hiOffset: Long, messages: Seq[KafkaMessageHeader])

/**
 *
 * @param responses
 */
case class OffsetResponse(responses: Map[String,Map[Int,PartitionOffsets]]) extends ProtocolResponse

object OffsetResponse extends Payload {
  def apply(data: ByteString): OffsetResponse = {
    val iterator = data.iterator
    val numTopics = iterator.getInt
    val responses: Map[String,Map[Int,PartitionOffsets]] = (0 until numTopics).map { _ =>
      val topicLen = iterator.getShort
      val topic = new Array[Byte](topicLen)
      iterator.getBytes(topic)
      val numPartitions = iterator.getInt
      val partitions: Map[Int,PartitionOffsets] = (0 until numPartitions).map { _ =>
        val partition = iterator.getInt
        val errorCode = ErrorCode(iterator.getShort)
        val numOffsets = iterator.getInt
        val offsets: Seq[Long] = (0 until numOffsets).map(_ => iterator.getLong)
        partition -> PartitionOffsets(partition, errorCode, offsets)
      }.toMap
      new String(topic, charset) -> partitions
    }.toMap
    OffsetResponse(responses)
  }
}

case class PartitionOffsets(partition: Int, errorCode: ErrorCode.ErrorCode, offsets: Seq[Long])
