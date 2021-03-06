package com.syntaxjockey.scafka

import akka.actor._
import akka.io.Tcp
import akka.io.Tcp._
import akka.io.IO
import akka.util.{ByteString, ByteStringBuilder}
import scala.concurrent.duration._
import java.util.concurrent.atomic.AtomicInteger
import java.net.InetSocketAddress
import java.nio.ByteOrder

import com.syntaxjockey.scafka.KafkaWorker.{KafkaWorkerState,KafkaWorkerData}

class KafkaWorker(broker: KafkaBroker, correlationCounter: AtomicInteger) extends LoggingFSM[KafkaWorkerState,KafkaWorkerData] with ActorLogging {
  import KafkaWorker._
  import context.system
  import context.dispatcher

  // config
  val sockaddr = new InetSocketAddress(broker.host, broker.port)
  val flushInterval = 3.seconds
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  startWith(WorkerUnconnected, ConnectionPending(1, Seq.empty))
  self ! ConnectToBroker

  when(WorkerUnconnected) {

    case Event(ConnectToBroker, ConnectionPending(attempts, requests)) =>
      IO(Tcp) ! Connect(sockaddr)
      stay()

    case Event(CommandFailed(cmd), ConnectionPending(attempts, requests)) =>
      log.error("failed to connect: {}", cmd.failureMessage.toString)
      context.system.scheduler.scheduleOnce(3 seconds, self, ConnectToBroker)
      stay() using ConnectionPending(attempts + 1, requests)

    case Event(request: KafkaRequest, ConnectionPending(attempts, requests)) =>
      log.debug("buffering request")
      stay() using ConnectionPending(attempts, requests :+ InFlightRequest(sender, request))

    case Event(Connected(remote, local), ConnectionPending(attempts, requests)) =>
      val io = sender
      log.info("connected to broker {}:{}", remote.getHostName, remote.getPort)
      io ! Register(self)
      goto(WorkerConnected) using ConnectedToBroker(io, requests, Map.empty, ByteString.empty, None)
  }

  onTransition {
    case WorkerUnconnected -> WorkerConnected =>
      context.parent ! BrokerConnected(broker.node)
      nextStateData match {
      case ConnectedToBroker(_, requests, _, _, _) =>
        if (!requests.isEmpty)
          self ! FlushRequests
      case _ =>
    }
  }

  when(WorkerConnected) {

    case Event(request: KafkaRequest, ConnectedToBroker(io, pending, inflight, leftover, flushTimer)) =>
      log.debug("buffering request")
      stay() using ConnectedToBroker(io, pending :+ InFlightRequest(sender, request), inflight, leftover, flushTimer)

    case Event(FlushRequests, ConnectedToBroker(_, pending, _, _, _)) if pending.isEmpty =>
      log.debug("flush was requested, but no requests are pending")
      stay()

    case Event(FlushRequests, ConnectedToBroker(io, pending, inflight, leftover, flushTimer)) =>
      log.debug("flushing {} pending requests", pending.length)
      val builder = new ByteStringBuilder()
      val sent: Map[Int,InFlightRequest] = pending.map { case InFlightRequest(_sender, request) =>
        val size = request.payloadSize
        log.debug("{} payload is {} bytes", request, size)
        builder.putInt(request.payloadSize)
        request.writePayload(builder)
        request.correlationId -> InFlightRequest(_sender, request)
      }.toMap
      val bytes = builder.result()
      io ! Write(bytes)
      log.debug("wrote {} requests in {} bytes", pending.length, bytes.length)
      val nextFlush = flushTimer.getOrElse(context.system.scheduler.scheduleOnce(flushInterval, self, FlushRequests))
      log.debug("scheduled next flush in {}", flushInterval)
      stay() using ConnectedToBroker(io, Seq.empty, inflight ++ sent, leftover, Some(nextFlush))

    case Event(Received(data), ConnectedToBroker(io, pending, inflight, leftover, flushTimer)) =>
      log.debug("received {} bytes from broker", data.length)
      var toRead = leftover ++ data
      var messageSize = 0
      try {
        while (toRead.length >= 4) {
          val messageIterator = toRead.iterator
          messageSize = messageIterator.getInt
          val messageBytes = messageIterator.toByteString
          if (messageBytes.length >= messageSize) {
            val responseIterator = messageBytes.iterator
            val correlationId = responseIterator.getInt
            val responseBytes = responseIterator.toByteString
            toRead = responseBytes.drop(messageSize - 4)
            inflight.get(correlationId) match {
              case Some(InFlightRequest(_sender, request)) =>
                _sender ! request.request.readResponse(responseBytes.take(messageSize - 4))
              case None => None
              // ignore
            }
          }
        }
      } catch {
        case ex: NoSuchElementException =>
          log.debug("{} bytes leftover after parsing", toRead.length)
      }
      stay() using ConnectedToBroker(io, pending, inflight, toRead, flushTimer)
  }
}

object KafkaWorker {
  def props(broker: KafkaBroker, correlationCounter: AtomicInteger) = Props(classOf[KafkaWorker], broker, correlationCounter)
  
  case class InFlightRequest(sender: ActorRef, request: KafkaRequest)

  case object ConnectToBroker
  case object GetConnectionState
  case object FlushRequests
  case object WriteAcknowledged extends Event

  sealed trait KafkaWorkerState
  case object WorkerUnconnected extends KafkaWorkerState
  case object WorkerConnected extends KafkaWorkerState
  case object WritePending extends KafkaWorkerState

  sealed trait KafkaWorkerData
  case class ConnectionPending(attempts: Int, requests: Seq[InFlightRequest]) extends KafkaWorkerData
  case class ConnectedToBroker(io: ActorRef, pending: Seq[InFlightRequest], inflight: Map[Int,InFlightRequest], leftover: ByteString, flushTimer: Option[Cancellable]) extends KafkaWorkerData
  case class ProcessingWrite(io: ActorRef, write: Write) extends KafkaWorkerData
}

case class KafkaBroker(node: Int, host: String, port: Int)

case class BrokerConnected(id: Int)
case class BrokerDisconnected(id: Int)

