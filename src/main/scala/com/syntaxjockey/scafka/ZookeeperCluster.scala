package com.syntaxjockey.scafka

import akka.actor._
import com.netflix.curator.framework.CuratorFramework
import com.netflix.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener, PathChildrenCache}
import com.netflix.curator.framework.recipes.cache.PathChildrenCache.StartMode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.collection.JavaConversions._
import scala.beans.BeanProperty
import java.util.concurrent.atomic.AtomicInteger

/**
*
* @param client
*/
class ZookeeperCluster(client: CuratorFramework) extends Actor with ActorLogging {

  // json object mapper
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  // config
  val minBrokers = 1

  val correlationCounter = new AtomicInteger()
  val brokerIds = new PathChildrenCache(client, "/brokers/ids", true)
  brokerIds.getListenable.addListener(new BrokersListener(self))
  brokerIds.start(StartMode.POST_INITIALIZED_EVENT)

  var unconnected = Map.empty[Int,ActorRef]
  var connected = Map.empty[Int,ActorRef]

  def receive = {

    case event: PathChildrenCacheEvent if event.getType == PathChildrenCacheEvent.Type.INITIALIZED =>
      unconnected = brokerIds.getCurrentData.map { child =>
        val id = child.getPath.split('/').last.toInt
        val broker = mapper.readValue(child.getData, classOf[ZkKafkaBroker])
        val worker = context.actorOf(KafkaWorker.props(KafkaBroker(id, broker.host, broker.port), correlationCounter), "worker-%d".format(id))
        log.info("added broker {}", id)
        id -> worker
      }.toMap

    case BrokerConnected(id) =>
      unconnected = unconnected.get(id) match {
        case Some(worker) =>
          log.info("connected to broker {}", id)
          connected = connected ++ Map(id -> worker)
          unconnected - id
        case None =>
          unconnected
      }
      if (connected.size == minBrokers)
        context.parent ! ClusterAvailable

    case BrokerDisconnected(id) =>
      connected = connected.get(id) match {
        case Some(worker) =>
          log.info("disconnected from broker {}", id)
          unconnected = unconnected ++ Map(id -> worker)
          connected - id
        case None =>
          connected
      }
      if (connected.size == minBrokers - 1)
        context.parent ! ClusterUnavailable

    case event: PathChildrenCacheEvent if event.getType == PathChildrenCacheEvent.Type.CHILD_ADDED =>
      val child = event.getData
      val id = child.getPath.split('/').last.toInt
      val broker = mapper.readValue(child.getData, classOf[ZkKafkaBroker])
      val worker = context.actorOf(KafkaWorker.props(KafkaBroker(id, broker.host, broker.port), correlationCounter))
      log.info("added broker {}", id)
      unconnected = unconnected ++ Map(id -> worker)

    case event: PathChildrenCacheEvent if event.getType == PathChildrenCacheEvent.Type.CHILD_UPDATED =>
      val child = event.getData
      log.debug("broker {} changed: {}", child.getPath, new String(child.getData))

    case event: PathChildrenCacheEvent if event.getType == PathChildrenCacheEvent.Type.CHILD_REMOVED =>
      val child = event.getData
      val id = child.getPath.split('/').last.toInt
      connected.get(id) match {
        case Some(worker) =>
          worker ! PoisonPill
          log.info("removed broker {}", id)
          connected = connected - id
        case None =>
          unconnected.get(id) match {
            case Some(worker) =>
              worker ! PoisonPill
              log.info("removed broker {}", id)
              unconnected = unconnected - id
            case None =>
          }
      }

    case event: PathChildrenCacheEvent =>
      log.debug("zookeeper connection changed: {}", event.toString)
  }
}

object ZookeeperCluster {
  def props(client: CuratorFramework) = Props(classOf[ZookeeperCluster], client)
}

case object ClusterAvailable
case object ClusterUnavailable

class BrokersListener(cluster: ActorRef) extends PathChildrenCacheListener {
  def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent) {
    cluster ! event
  }
}

class ZkKafkaBroker {
  @BeanProperty var host: String = _
  @BeanProperty var port: Int = _
  @BeanProperty var jmx_port: Int = _
  @BeanProperty var version: Int = _
}

class ZkKafkaTopic {
  @BeanProperty var partitions: Map[String,Seq[Int]] = _
  @BeanProperty var version: Int = _
}