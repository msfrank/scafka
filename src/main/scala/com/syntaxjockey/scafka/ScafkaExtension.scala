package com.syntaxjockey.scafka

import akka.actor._

class ScafkaManager extends Actor with ActorLogging {

  //val config = context.system.settings.config.getConfig("scafka")

  // state
  var clusters: Map[String,ActorRef] = Map.empty

  log.debug("started scafka")

  def receive = {

    case RegisterCluster(id, brokers: Seq[KafkaBroker]) =>
      val cluster = clusters.get(id) match {
        case Some(actor) =>
          actor
        case None =>
          val actor = context.actorOf(KafkaCluster.props(id, brokers), id)
          clusters = clusters + (id -> actor)
          log.debug("registered cluster {}", id)
          actor
      }
      sender ! cluster

    case UnregisterCluster(broker) =>

  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    log.warning("restarted scafka")
  }

  override def postStop() {
    log.debug("stopped scafka")
  }
}

class ScafkaExtension(system: ActorSystem) extends Extension {
  val manager = system.actorOf(Props[ScafkaManager], "scafka-manager")
}

object Scafka extends ExtensionId[ScafkaExtension] with ExtensionIdProvider {

  override def lookup() = Scafka
  override def createExtension(system: ExtendedActorSystem) = new ScafkaExtension(system)

  /**
   * Get a reference to the Scafka manager actor.
   *
   * @param system
   * @return
   */
  def manager(implicit system: ActorSystem): ActorRef = super.get(system).manager
}

case class RegisterCluster(id: String, brokers: Seq[KafkaBroker])
case class UnregisterCluster(id: String)
