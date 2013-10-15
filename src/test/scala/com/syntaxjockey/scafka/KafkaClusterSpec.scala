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
      Scafka(system).manager ! RegisterCluster("cluster", Seq(KafkaBroker("broker", "localhost", 9092, 0)))
      val cluster = expectMsgClass(classOf[ActorRef])
    }

    "process a low-level ProduceRequest" in {
      Scafka(system).manager ! RegisterCluster("cluster", Seq(KafkaBroker("broker", "localhost", 9092, 0)))
      val cluster = expectMsgClass(classOf[ActorRef])
      val message = KafkaMessage(Some("key".getBytes), Some("value".getBytes), Attributes.CODEC_NONE, KafkaMessage.MAGIC)
      val request = ProduceRequest(Map("topic" -> Map(1 -> message)), 10 seconds, 1)
      cluster ! KafkaRequest(request, KafkaRequest.VERSION, 0, "client-id")
      val response = expectMsgClass(classOf[ProduceResponse])
    }
  }
}
