package kvstore

import akka.actor.Actor.Receive
import akka.actor.{ActorRef, ReceiveTimeout, Actor, Props}
import kvstore.Persistence.{Persist, Persisted}
import kvstore.PersistenceSupervisor.PersistenceFail
import scala.concurrent.duration._

/**
 * Created by eduardo.ponte on 02/06/2015.
 */

object PersistenceSupervisor {
  case class PersistenceFail(key: String, id: Long)

  def props[A](requestor: ActorRef, persistence: ActorRef, msg: Persist, response: A): Props = Props(classOf[PersistenceSupervisor[A]], requestor, persistence, msg, response)
}

class PersistenceSupervisor[A](requestor: ActorRef, persistence: ActorRef, msg: Persist, response: A) extends Actor {
  import context.dispatcher

  context.setReceiveTimeout(1.second)

  val cancellable = context.system.scheduler.schedule(0.millis, 100.millis, persistence, msg)

  override def receive: Receive = {
    case Persisted(key, seq) =>
      cancellable.cancel()
      requestor ! response

    case ReceiveTimeout => requestor ! PersistenceFail(msg.key, msg.id)
  }
}
