package com.syntaxjockey.scafka

import akka.actor.{ActorRef, Props, ActorLogging, Actor}
import akka.routing.SmallestMailboxRouter
import akka.serialization.Serializer
import java.util.concurrent.atomic.AtomicInteger

/**
 *
 * @param id
 * @param brokers
 */
class KafkaCluster(id: String, brokers: Seq[KafkaBroker]) extends Actor with ActorLogging {

  val correlationCounter = new AtomicInteger()
  val workers: Vector[ActorRef] = brokers.map { broker =>
    context.actorOf(KafkaWorker.props(broker, correlationCounter), broker.id)
  }.toVector
  var router = context.actorOf(Props.empty.withRouter(SmallestMailboxRouter(routees = workers)))

  def receive = {
    case request: KafkaRequest =>
      router forward request
  }
}

object KafkaCluster {
  def props(id: String, brokers: Seq[KafkaBroker]) = Props(classOf[KafkaCluster], id, brokers)
}

//case class ClusterParams(serializer: Serializer, partitioner: Partitioner)
//
//trait Partitioner[T] {
//  def getPartition(message: T): Int
//}
