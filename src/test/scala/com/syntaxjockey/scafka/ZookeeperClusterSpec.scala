package com.syntaxjockey.scafka

import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.ActorSystem
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import org.scalatest.matchers.MustMatchers
import com.typesafe.config.ConfigFactory
import akka.util.Timeout
import scala.concurrent.duration._

class ZookeeperClusterSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpec with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ZookeeperClusterSpec", ConfigFactory.parseString(
    """
      |scafka {
      |  zookeeper {
      |    seeds = ["localhost:2181"]
      |    session-timeout = 1 hour
      |    connection-timeout = 30 seconds
      |    max-retries = 5
      |    base-sleep-time = 5 seconds
      |    max-sleep-time = 1 minute
      |  }
      |}
    """.stripMargin)))

  "A ZookeeperCluster" must {

    "connect to all available brokers" in {
      val client = Zookeeper(system).client
      val cluster = system.actorOf(ZookeeperCluster.props(client))
      implicit val timeout = Timeout(10 seconds)
      expectMsg(ClusterAvailable)
    }
  }
}
