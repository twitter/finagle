package com.twitter.finagle.http2.transport.client

import com.twitter.finagle.Stack.Params
import com.twitter.finagle._
import com.twitter.finagle.client.{DefaultPool, EndpointerModule, StackClient}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.param.Stats
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.stack.Endpoint
import com.twitter.logging.Logger
import com.twitter.util._
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec

private[finagle] object H2Pool {

  private[transport] type OnH2Service = Service[Request, Response] => Unit

  /**
   * A `Stack.Param` that we use to link the transporter layer with the pooling
   * layer in the stack.
   *
   * For H2C and ALPN based H2 strategies we need a way for the multiplexed
   * `Transporter` implementations to pass a H2 session back to the pooling layer.
   * For this we send a callback function down through the stack params during
   * the stack initialization.
   */
  private[transport] case class OnH2ServiceParam(onH2Service: Option[OnH2Service])
  private[transport] case object OnH2ServiceParam {
    implicit val param: Stack.Param[OnH2ServiceParam] =
      Stack.Param(OnH2ServiceParam(None))
  }

  private sealed trait State
  private object State {
    case object Closed extends State
    case object H1 extends State
    final case class H2(session: ServiceFactory[Request, Response]) extends State
  }

  private[this] object H2PoolModule extends Stack.Module[ServiceFactory[Request, Response]] {

    def role: Stack.Role = StackClient.Role.pool

    def description: String = "H2 compatible session pool"

    def parameters: Seq[Stack.Param[_]] = H2Pool.parameters

    def make(
      params: Params,
      next: Stack[ServiceFactory[Request, Response]]
    ): Stack[ServiceFactory[Request, Response]] = Stack.leaf(role, new H2Pool(params, next))
  }

  private val logger = Logger(classOf[H2Pool])

  private val parameters =
    SingletonPool.module(false).parameters ++
      DefaultPool.module.parameters

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[H2Pool]].
   */
  def module: Stackable[ServiceFactory[Request, Response]] = H2PoolModule
}

/**
 * A pool implementation that can switch pooling methods.
 *
 * We'd like to use the SingletonPool for H2 but for TLS and H2C HTTP/2
 * clients we are not guaranteed a H2 session. This pool implementation
 * is backed by either of two pools depending on the outcome: initially
 * we get the standard pool stack but if we get a H2 session we then
 * switch to a SingletonPool for the lifetime of the session. After that
 * we go back to the stand pool stack, and repeat.
 */
private final class H2Pool(params: Params, stackFragment: Stack[ServiceFactory[Request, Response]])
    extends ServiceFactory[Request, Response] {

  import H2Pool._

  private[this] val state = new AtomicReference[State](State.H1)

  // This gauge will be nice during the rollout for debugging but it may
  // become DEBUG scoped or removed altogether after the rollout phase.
  private[this] val sessionGauge = params[Stats].statsReceiver
    .addGauge("h2pool-sessions") {
      state.get match {
        case State.H2(_) => 1.0f
        case _ => 0.0f
      }
    }

  @tailrec
  private[this] final def onH2Service(h2Session: Service[Request, Response]): Unit =
    state.get match {
      case State.Closed =>
        h2Session.close() // Don't need it.

      case h @ State.H2(old) =>
        // We can't just set it directly since we need to send it through `makeSingleton`
        // so we try to set ourselves to H1 and reuse the logic below
        if (old.status == Status.Closed && state.compareAndSet(h, State.H1)) {
          old.close()
          onH2Service(h2Session)
        } else {
          h2Session.close()
        }

      case State.H1 =>
        makeSingleton(h2Session).respond {
          case Return(singleton) =>
            if (!state.compareAndSet(State.H1, State.H2(singleton)))
              singleton.close()

          case Throw(_) => h2Session.close()
        }
    }

  // We don't make the H1 session part of the state machine to keep at least one
  // pathway stable so if our H2 session goes down we're not racing to build
  // factories etc, we just use the standard pool. Presumably it will get ignored
  // for the majority of the time in favor of a H2 session and the members of the
  // pool will timeout soon enough so it shouldn't be a huge burden to keep around.
  // This also results in a reduction in complexity of the state machine.
  private[this] val defaultPool: ServiceFactory[Request, Response] =
    (DefaultPool.module[Request, Response] +: stackFragment)
      .make(params + OnH2ServiceParam(Some(onH2Service)))

  @tailrec
  def apply(conn: ClientConnection): Future[Service[Request, Response]] = {
    state.get match {
      case h2 @ State.H2(singleton) =>
        if (singleton.status != Status.Closed) {
          logger.debug("Using live H2 session.")
          singleton(conn)
        } else {
          // Our H2 session is no longer available. Try to close the
          // resource, reset our state, and try again likely resulting
          // in using the default pool unless we lost a race.
          if (state.compareAndSet(h2, State.H1)) {
            logger.debug("Closed H2 session.")
            singleton.close()
          }
          apply(conn)
        }

      case _ =>
        // We're either H1 or closed, either way the default pool
        // will handle this for us.
        defaultPool(conn)
    }
  }

  override def status: Status = state.get match {
    case State.H2(singleton) =>
      singleton.status match {
        case Status.Closed => defaultPool.status
        case other => other
      }

    case State.H1 => defaultPool.status

    case State.Closed => Status.Closed
  }

  def close(deadline: Time): Future[Unit] = {
    sessionGauge.remove()

    state.getAndSet(State.Closed) match {
      case State.Closed =>
        // already closed.
        Future.Done
      case State.H1 =>
        // We only have the default pool to close
        defaultPool.close(deadline)
      case State.H2(singleton) =>
        // We need to close both the h2 singleton and the default pool
        Closable.all(defaultPool, singleton).close(deadline)
    }
  }

  private[this] def makeSingleton(
    h2Session: Service[Request, Response]
  ): Future[ServiceFactory[Request, Response]] = {
    val endpointer =
      new EndpointerModule[Request, Response](
        Nil,
        (_, _) => ServiceFactory(() => Future.value(h2Session)))
    val svcFactory: ServiceFactory[Request, Response] =
      stackFragment
        .replace(Endpoint, endpointer)
        .make(params)

    // Get a (future) service wrapped in all the intermediate ServiceProxy
    // instances from the bottom of the stack.
    val singletonService: Future[Service[Request, Response]] = svcFactory()

    // Wrap our singleton service in a `RefCountedFactory` so that
    // it can be safely shared.
    singletonService.map(new RefCountedFactory[Request, Response](_))
  }
}
