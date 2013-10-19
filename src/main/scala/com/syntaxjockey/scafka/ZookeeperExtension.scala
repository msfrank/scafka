package com.syntaxjockey.scafka

import akka.actor._
import com.netflix.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import com.netflix.curator.retry.BoundedExponentialBackoffRetry
import scala.collection.JavaConversions._

class ZookeeperExtension(system: ActorSystem) extends Extension {
  val config = system.settings.config.getConfig("scafka.zookeeper")
  val client = {
    val connectString = config.getStringList("seeds").mkString(",")
    val sessionTimeout = config.getMilliseconds("session-timeout")
    val connectionTimeout = config.getMilliseconds("connection-timeout")
    val retryBaseSleep = config.getMilliseconds("base-sleep-time")
    val retryMaxSleep = config.getMilliseconds("max-sleep-time")
    val maxRetries = config.getInt("max-retries")
    val retryPolicy = new BoundedExponentialBackoffRetry(retryBaseSleep.toInt, retryMaxSleep.toInt, maxRetries)
    val client = CuratorFrameworkFactory.newClient(connectString, sessionTimeout.toInt, connectionTimeout.toInt, retryPolicy)
    if (config.hasPath("namespace")) client.usingNamespace(config.getString("namespace")) else client
  }
  client.start()
}

object Zookeeper extends ExtensionId[ZookeeperExtension] with ExtensionIdProvider {

  override def lookup() = Zookeeper
  override def createExtension(system: ExtendedActorSystem) = new ZookeeperExtension(system)

  /**
   * Get a reference to the CuratorFramework.
   *
   * @param system
   * @return
   */
  def client(implicit system: ActorSystem): CuratorFramework = super.get(system).client
}
