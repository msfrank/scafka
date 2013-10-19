package com.syntaxjockey.scafka

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import org.scalatest.matchers.MustMatchers
import scala.concurrent.duration._

class KafkaClusterSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpec with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("KafkaClusterSpec"))

  override def afterAll() {
    system.shutdown()
  }

  "KafkaCluster" must {

    "register a cluster programmatically" in {
      Scafka(system).manager ! RegisterCluster("cluster", Seq(KafkaBroker(1, "localhost", 9092)))
      val cluster = expectMsgClass(classOf[ActorRef])
    }

    "process a low-level MetadataRequest for all topics" in {
      Scafka(system).manager ! RegisterCluster("cluster", Seq(KafkaBroker(1, "localhost", 9092)))
      val cluster = expectMsgClass(classOf[ActorRef])
      val request = MetadataRequest(Seq.empty)
      cluster ! KafkaRequest(request, KafkaRequest.VERSION, 0, "client-id")
      val response = expectMsgClass(classOf[MetadataResponse])
      println(response)
    }

    "process a low-level MetadataRequest for a single topic which exists" in {
      Scafka(system).manager ! RegisterCluster("cluster", Seq(KafkaBroker(1, "localhost", 9092)))
      val cluster = expectMsgClass(classOf[ActorRef])
      val request = MetadataRequest(Seq("topicexists"))
      cluster ! KafkaRequest(request, KafkaRequest.VERSION, 0, "client-id")
      val response = expectMsgClass(classOf[MetadataResponse])
      println(response)
    }

    "process a low-level ProduceRequest" in {
      Scafka(system).manager ! RegisterCluster("cluster", Seq(KafkaBroker(1, "localhost", 9092)))
      val cluster = expectMsgClass(classOf[ActorRef])
      val message = KafkaMessage(Some("key".getBytes), Some("value".getBytes), Attributes.CODEC_NONE, KafkaMessage.MAGIC)
      val request = ProduceRequest(Map("topicexists" -> Map(0 -> message)), 10 seconds, 1)
      cluster ! KafkaRequest(request, KafkaRequest.VERSION, 0, "client-id")
      val response = expectMsgClass(classOf[ProduceResponse])
      println(response)
      for ((id,partition) <- response.responses("topicexists")) {
        partition.errorCode must be(ErrorCode.NoError)
      }
    }

    "process a low-level FetchRequest" in {
      Scafka(system).manager ! RegisterCluster("cluster", Seq(KafkaBroker(1, "localhost", 9092)))
      val cluster = expectMsgClass(classOf[ActorRef])
      val request = FetchRequest(FetchRequest.REPLICA_UNSPECIFIED, 1 second, 1, Map("topicexists" -> Seq(FetchPartition(0, 0, 65535))))
      cluster ! KafkaRequest(request, KafkaRequest.VERSION, 0, "client-id")
      val response = expectMsgClass(classOf[FetchResponse])
      println(response)
      for ((id,partition) <- response.responses("topicexists")) {
        partition.errorCode must be(ErrorCode.NoError)
      }
    }

    "process a low-level OffsetRequest" in {
      Scafka(system).manager ! RegisterCluster("cluster", Seq(KafkaBroker(1, "localhost", 9092)))
      val cluster = expectMsgClass(classOf[ActorRef])
      val request = OffsetRequest(0, Map("topicexists" -> Map(0 -> RequestOffsets(0, OffsetRequest.EARLIEST_OFFSETS, 64))))
      cluster ! KafkaRequest(request, KafkaRequest.VERSION, 0, "client-id")
      val response = expectMsgClass(classOf[OffsetResponse])
      println(response)
      for ((id,partition) <- response.responses("topicexists")) {
        partition.errorCode must be(ErrorCode.NoError)
      }
    }

  }
}
