package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  case class ReplicationCleanup(id: Long)

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 5) {
    case _: Exception => SupervisorStrategy.Restart
  }

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  val persistence = context.actorOf(persistenceProps)

  arbiter ! Join

  var replicationReqs = Map.empty[Long,(ActorRef, Int, Cancellable)]

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Get(k,id) => sender ! GetResult(k,kv.get(k),id)

    case Insert(k,v,id) =>
      val cancellableReplication = context.system.scheduler.scheduleOnce(1.second, self, ReplicationCleanup(id))
      replicationReqs += (id -> (sender,0,cancellableReplication))
      kv += ((k,v))
      replicators.foreach(_ ! Replicate(k,Some(v),id))
      context.actorOf(PersistenceSupervisor.props(self, persistence, Persist(k, Some(v), id), Replicated(k,id)))

    case Replicated(k,id) =>
      val replicationReq = replicationReqs(id)
      replicationReq match {
        case (client, acksReceived, cancellable) =>
          if (acksReceived < replicators.size - 1) {
            replicationReqs += (id -> (client,acksReceived + 1, cancellable))
          } else {
            cancellable.cancel()
            replicationReqs -= id
            client ! OperationAck(id)
          }
        case _ =>
      }

    case ReplicationCleanup(id) =>
      val replicationReq = replicationReqs(id)
      replicationReq match {
        case (client, acksReceived, cancellable) =>
          replicationReqs -= id
          client ! OperationFailed(id)
        case _ =>
      }

    case Remove(k,id) =>
      val cancellableReplication = context.system.scheduler.scheduleOnce(1.second, self, ReplicationCleanup(id))
      replicationReqs += (id -> (sender,0,cancellableReplication))
      kv -= k
      replicators.foreach(_ ! Replicate(k,None,id))
      context.actorOf(PersistenceSupervisor.props(self, persistence, Persist(k, None, id), Replicated(k,id)))

    case Replicas(replicas) =>
      val deadSet = secondaries.keySet -- replicas
      deadSet.foreach { rep =>
        rep ! PoisonPill
        secondaries(rep) ! PoisonPill
      }
      val newSet =  replicas -- secondaries.keySet
      val newSecondaries = newSet.map(replica => replica -> context.actorOf(Replicator.props(replica))).toMap
      var id = 0L
      for {
        (k,v) <- kv
      } yield {
        val cancellableReplication = context.system.scheduler.scheduleOnce(1.second, self, ReplicationCleanup(id))
        replicationReqs += (id -> (sender,0,cancellableReplication))
        newSecondaries.values.foreach(_ ! Replicate(k,Some(v),id))
        context.actorOf(PersistenceSupervisor.props(self, persistence, Persist(k, None, id), Replicated(k,id)))
        id += 1L
      }

      replicators = replicators ++ newSecondaries.values -- deadSet
      secondaries = secondaries ++ newSecondaries -- deadSet
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(k,id) => sender ! GetResult(k,kv.get(k),id)
    case Snapshot(k,v,seq) => manageSnapshot(k,v,seq)
    case _ =>
  }

  var snapshotSeq = List.empty[Long]
  def manageSnapshot(k: String, v: Option[String], seq: Long): Unit = {
    if (snapshotSeq.isEmpty && seq != 0) {
      // swallow: no state change, no reaction
    } else if ((snapshotSeq.isEmpty && seq == 0) || (!snapshotSeq.contains(seq) && snapshotSeq.forall(x => seq > x) && seq == snapshotSeq.head + 1)) {
      v match {
        case Some(vin) => kv += ((k,vin))
        case None => kv -= k
      }
      snapshotSeq = seq :: snapshotSeq
      context.actorOf(PersistenceSupervisor.props(sender, persistence, Persist(k, v, seq), SnapshotAck(k, seq)))
    } else {
      // smaller than expected: this is from "the past", send an ack
      sender ! SnapshotAck(k, seq)
    }
  }
}

