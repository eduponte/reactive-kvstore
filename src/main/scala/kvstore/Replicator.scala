package kvstore

import akka.actor.{Cancellable, Props, Actor, ActorRef}
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var seqs = Map.empty[Long, Long]
  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate, Cancellable)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }


  def replicate(k: String, v: Option[String], id: Long, msg: Replicate): Unit = {
    val cancellableRetry = context.system.scheduler.scheduleOnce(200 milliseconds) {
      self ! Replicate(k, v, id)
    }

    seqs.get(id) match {
      case Some(seq) => {
        val ack = acks(seq)
        acks.updated(seq,(ack._1, ack._2, cancellableRetry)) // replace with new cancellable
        replica ! Snapshot(k, v, seq)
      }
      case None => {
        acks += _seqCounter -> ((sender, msg, cancellableRetry))
        seqs += ((id, _seqCounter))
        replica ! Snapshot(k, v, _seqCounter)
        nextSeq
      }
    }
  }

  def confirmReplication(seq: Long): Unit = {
    acks.get(seq) match {
      case Some(ack) => {
        acks -= seq
        ack._3.cancel
        ack._1 ! Replicated(ack._2.key, ack._2.id)
      }
      case None =>
    }
  }

  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case replicateMsg @ Replicate(k, v, id) => replicate(k, v, id, replicateMsg)
    case SnapshotAck(key, seq) => confirmReplication(seq)
  }


}
