package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.service.FailingFactory
import com.twitter.util.Future
import com.twitter.util.Time
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.util.control.NonFatal

/**
 * A specialized [[ServiceFactory]] which admits that it backs a
 * concrete endpoint. The extra information and functionality provided
 * here is used by Finagle's load balancers.
 */
trait EndpointFactory[Req, Rep] extends ServiceFactory[Req, Rep] {

  /**
   * Returns the address which this endpoint connects to.
   */
  val address: Address

  private[loadbalancer] lazy val weight: Double = WeightedAddress.extract(address)._2

  /**
   * Signals to the endpoint that it should close and rebuild
   * its underlying resources. That is, `close` is terminal
   * but `remake` is not.
   */
  def remake(): Unit
}

/**
 * An [[EndpointFactory]] that fails to construct services.
 */
private final class FailingEndpointFactory[Req, Rep](cause: Throwable)
    extends EndpointFactory[Req, Rep] {
  val address: Address = Address.Failed(cause)
  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = Future.exception(cause)
  def close(deadline: Time): Future[Unit] = Future.Done
  def remake(): Unit = {}
  override def status: Status = Status.Open
}

private object LazyEndpointFactory {

  private class EndpointAlreadyClosedException(cause: Exception) extends ServiceClosedException {
    initCause(cause)
    override def getMessage: String = "Tried to acquire endpoint after it was closed"
  }

  private class EndpointMarkedClosedException(address: Address) extends ServiceClosedException {
    override def getMessage: String = s"Endpoint $address was marked closed"
  }

  sealed trait State[-Req, +Rep]

  /**
   * Indicates that the underlying resource needs to be materialized.
   */
  case object Init extends State[Any, Nothing]

  /**
   * Indicates that the EndpointFactory is closed and will no longer
   * admit any service acquisition requests.
   */
  case class Closed(exn: ServiceClosedException) extends State[Any, Nothing]

  /**
   * Indicates that the process of building the underlying resources
   * is in progress.
   */
  case object Making extends State[Any, Nothing]

  /**
   * Indicates that the EndpointFactory has a materialized backing
   * resource which it will proxy service acquisition requests to.
   */
  case class Made[Req, Rep](underlying: ServiceFactory[Req, Rep]) extends State[Req, Rep]
}

/**
 * An implementation of [[EndpointFactory]] which is lazy. That is, it delays
 * the creation of its implementation until it receives the first service acquisition
 * request. This is designed to allow the load balancer to construct the stacks
 * for a large collection of endpoints and amortize the cost across requests.
 * Note, this isn't related to session establishment. Session establishment is
 * lazy and on the request path already, but rather creating a large number of
 * objects per namer updates can be expensive.
 */
private final class LazyEndpointFactory[Req, Rep](
  mk: () => ServiceFactory[Req, Rep],
  val address: Address)
    extends EndpointFactory[Req, Rep] {
  import LazyEndpointFactory._

  private[this] val state = new AtomicReference[State[Req, Rep]](Init)

  @tailrec def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
    state.get match {
      case Init =>
        if (state.compareAndSet(Init, Making)) {
          val underlying =
            try mk()
            catch {
              case NonFatal(exc) =>
                new FailingFactory[Req, Rep](exc)

              case fatal: Throwable =>
                // We must not leave the lock in an inconsistent `Making` state, even
                // for fatal exceptions, so we take a conservative approach of replacing
                // the `Init` state and then rethrowing the exception.
                state.set(Init)
                throw fatal
            }
          // This is the only place where we can transition from `Making`
          // to any other state so this is safe. All other spin loops wait
          // for the thread that has entered here to exit the `Making`
          // state.
          state.set(Made(underlying))
        }
        apply(conn)

      case Making => apply(conn)
      case Made(underlying) => underlying(conn)
      case Closed(cause) => Future.exception(new EndpointAlreadyClosedException(cause))
    }

  /**
   * Returns the underlying [[ServiceFactory]] if it is
   * materialized otherwise None. This is useful for testing.
   */
  def self: Option[ServiceFactory[Req, Rep]] = state.get match {
    case Made(underlying) => Some(underlying)
    case _ => None
  }

  @tailrec def remake(): Unit = state.get match {
    case Init | Closed(_) => // nop
    case Making => remake()
    case s @ Made(underlying) =>
      // Note, underlying is responsible for draining any outstanding
      // service acquisition requests gracefully.
      if (!state.compareAndSet(s, Init)) remake()
      else underlying.close()
  }

  @tailrec def close(when: Time): Future[Unit] = state.get match {
    case Closed(_) => Future.Done
    case Making => close(when)
    case Init =>
      if (!state.compareAndSet(Init, Closed(new EndpointMarkedClosedException(address))))
        close(when)
      else Future.Done
    case s @ Made(underlying) =>
      if (!state.compareAndSet(s, Closed(new EndpointMarkedClosedException(address)))) close(when)
      else underlying.close(when)
  }

  override def status: Status = state.get match {
    case Init | Making => Status.Open
    case Closed(_) => Status.Closed
    case Made(underlying) => underlying.status
  }

  override def toString: String = s"EndpointFactory(addr=$address, status=$status)"
}

/**
 * A proxy masks the underlying EndpointFactory to make it not closable.
 * The endpoint passed in here is shared among load balancers, it is reusable and should
 * not be closed by any balancer or LoadBalancerFactory.
 *
 * Note: `factory` should be managed by the above layers to ensure we release the resources and
 * close reusable endpoints eventually.
 */
private[finagle] final case class NotClosableEndpointFactoryProxy[Req, Rep](
  factory: EndpointFactory[Req, Rep])
    extends EndpointFactory[Req, Rep] {

  override def status: Status = factory.status

  val address: Address = factory.address

  def remake(): Unit = factory.remake()

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = factory(conn)

  def close(deadline: Time): Future[Unit] = Future.Done
}
