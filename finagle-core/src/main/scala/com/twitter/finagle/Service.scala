package com.twitter.finagle

import com.twitter.finagle.util.InetSocketAddressUtil.unconnected
import com.twitter.util.{Closable, Future, NonFatal, Time}
import java.net.SocketAddress

object Service {

  /**
   * Wrap an underlying service such that any synchronously thrown exceptions are lifted into
   * Future.exception
   */
  def rescue[Req, Rep](service: Service[Req, Rep]) = new ServiceProxy[Req, Rep](service) {
    override def apply(request: Req): Future[Rep] = {
      try {
        service(request)
      } catch {
        case NonFatal(e) => Future.exception(e)
      }
    }
  }

  def mk[Req, Rep](f: Req => Future[Rep]): Service[Req, Rep] = new Service[Req, Rep] {
    def apply(req: Req): Future[Rep] = f(req)
  }

  /**
   * A service with a constant reply. Always available; never closable.
   *
   * @see [[constant]] for a Java compatible API.
   */
  def const[Rep](rep: Future[Rep]): Service[Any, Rep] =
    new service.ConstantService(rep)

  /** Java compatible API for [[const]] as `const` is a reserved word in Java */
  def constant[Rep](rep: Future[Rep]): Service[Any, Rep] =
    Service.const(rep)
}

/**
 * A Service is an asynchronous function from Request to Future[Response]. It is the
 * basic unit of an RPC interface.
 *
 * '''Note:''' this is an abstract class (vs. a trait) to maintain java
 * compatibility, as it has implementation as well as interface.
 */
abstract class Service[-Req, +Rep] extends (Req => Future[Rep]) with Closable {
  def map[Req1](f: Req1 => Req) = new Service[Req1, Rep] {
    def apply(req1: Req1): Future[Rep] = Service.this.apply(f(req1))
    override def close(deadline: Time): Future[Unit] = Service.this.close(deadline)
  }

  /**
   * This is the method to override/implement to create your own Service.
   */
  def apply(request: Req): Future[Rep]

  /**
   * Relinquishes the use of this service instance. Behavior is
   * undefined if apply() is called after resources are relinquished.
   */
  // This is asynchronous on purpose, the old API allowed for it.
  @deprecated("Use close() instead", "7.0.0")
  final def release(): Unit = close()

  def close(deadline: Time): Future[Unit] = Future.Done

  /**
   * The current availability [[Status]] of this Service.
   */
  def status: Status = Status.Open

  /**
   * Determines whether this service is available (can accept requests
   * with a reasonable likelihood of success).
   */
  final def isAvailable: Boolean = status == Status.Open
}

/**
 * Information about a client, passed to a Service factory for each new
 * connection.
 */
trait ClientConnection extends Closable {
  /**
   * Host/port of the client. This is only available after `Service#connected`
   * has been signalled.
   */
  def remoteAddress: SocketAddress

  /**
   * Host/port of the local side of a client connection. This is only
   * available after `Service#connected` has been signalled.
   */
  def localAddress: SocketAddress

  /**
   * Expose a Future[Unit] that will be filled when the connection is closed
   * Useful if you want to trigger action on connection closing
   */
  def onClose: Future[Unit]
}

object ClientConnection {
  val nil: ClientConnection = new ClientConnection {
    def remoteAddress: SocketAddress = unconnected
    def localAddress: SocketAddress = unconnected
    def close(deadline: Time): Future[Unit] = Future.Done
    def onClose: Future[Unit] = Future.never
  }
}

/**
 * A simple proxy Service that forwards all calls to another Service.
 * This is useful if you want to wrap-but-modify an existing service.
 */
abstract class ServiceProxy[-Req, +Rep](val self: Service[Req, Rep])
  extends Service[Req, Rep] with Proxy
{
  def apply(request: Req): Future[Rep] = self(request)
  override def close(deadline: Time): Future[Unit] = self.close(deadline)

  override def status: Status = self.status

  override def toString: String = self.toString
}

abstract class ServiceFactory[-Req, +Rep]
  extends (ClientConnection => Future[Service[Req, Rep]])
  with Closable
{ self =>

  /**
   * Reserve the use of a given service instance. This pins the
   * underlying channel and the returned service has exclusive use of
   * its underlying connection. To relinquish the use of the reserved
   * Service, the user must call Service.close().
   */
  def apply(conn: ClientConnection): Future[Service[Req, Rep]]
  final def apply(): Future[Service[Req, Rep]] = this(ClientConnection.nil)

  @deprecated("use apply() instead", "5.0.1")
  final def make(): Future[Service[Req, Rep]] = this()

  /**
   * Apply `f` on created services, returning the resulting Future in their
   * stead. This is useful for implementing common factory wrappers that
   * only need to modify or operate on the underlying service.
   */
  def flatMap[Req1, Rep1](f: Service[Req, Rep] => Future[Service[Req1, Rep1]]): ServiceFactory[Req1, Rep1] =
    new ServiceFactory[Req1, Rep1] {
      def apply(conn: ClientConnection): Future[Service[Req1, Rep1]] =
        self(conn) flatMap { service =>
          f(service) onFailure { _ => service.close() }
        }
      def close(deadline: Time) = self.close(deadline)
      override def status: Status = self.status
      override def toString(): String = self.toString()
    }

  /**
   * Map created services. Useful for implementing common
   * styles of factory wrappers.
   */
  def map[Req1, Rep1](f: Service[Req, Rep] => Service[Req1, Rep1]): ServiceFactory[Req1, Rep1] =
    flatMap { s => Future.value(f(s)) }

  /**
   * Make a service that after dispatching a request on that service,
   * releases the service.
   */
  final def toService: Service[Req, Rep] = new FactoryToService(this)

  /**
   * The current availability [[Status]] of this ServiceFactory
   */
  def status: Status = Status.Open

  final def isAvailable: Boolean = status == Status.Open
}

object ServiceFactory {
  def const[Req, Rep](service: Service[Req, Rep]): ServiceFactory[Req, Rep] =
    new ServiceFactory[Req, Rep] {
      private[this] val noRelease = Future.value(new ServiceProxy[Req, Rep](service) {
        // close() is meaningless on connectionless services.
        override def close(deadline: Time) = Future.Done
      })

      def apply(conn: ClientConnection): Future[Service[Req, Rep]] = noRelease
      def close(deadline: Time): Future[Unit] = Future.Done
    }

  def apply[Req, Rep](f: () => Future[Service[Req, Rep]]): ServiceFactory[Req, Rep] =
    new ServiceFactory[Req, Rep] {
      def apply(_conn: ClientConnection): Future[Service[Req, Rep]] = f()
      def close(deadline: Time): Future[Unit] = Future.Done
    }
}

@deprecated("use ServiceFactoryProxy instead", "6.7.5")
trait ProxyServiceFactory[-Req, +Rep] extends ServiceFactory[Req, Rep] with Proxy {
  def self: ServiceFactory[Req, Rep]
  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = self(conn)
  def close(deadline: Time): Future[Unit] = self.close(deadline)

  override def status: Status = self.status
}

/**
 * A simple proxy ServiceFactory that forwards all calls to another
 * ServiceFactory.  This is is useful if you to wrap-but-modify an
 * existing service factory.
 */
abstract class ServiceFactoryProxy[-Req, +Rep](_self: ServiceFactory[Req, Rep])
  extends ProxyServiceFactory[Req, Rep] {
  def self: ServiceFactory[Req, Rep] = _self
}

private[finagle] case class ServiceFactorySocketAddress[Req, Rep](factory: ServiceFactory[Req, Rep])
  extends SocketAddress

object FactoryToService {
  val role = Stack.Role("FactoryToService")

  // TODO: we should simply transform the stack for boolean
  // stackables like this.
  case class Enabled(enabled: Boolean) {
    def mk(): (Enabled, Stack.Param[Enabled]) =
      (this, Enabled.param)
  }
  object Enabled {
    implicit val param = Stack.Param(Enabled(false))
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]]
   * [[FactoryToService]]. This makes per-request service acquisition
   * part of the stack so it can be wrapped by filters such as tracing.
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[Enabled, ServiceFactory[Req, Rep]] {
      val role = FactoryToService.role
      val description = "Apply service factory on each service request"
      def make(_enabled: Enabled, next: ServiceFactory[Req, Rep]) = {
        if (_enabled.enabled) {
          /*
           * The idea here is to push FactoryToService down the stack
           * so that service acquisition in the course of a request is
           * wrapped by tracing, timeouts, etc. for that request.
           *
           * We can't return a Service directly (since the stack type
           * is ServiceFactory); instead we wrap it in a
           * ServiceFactoryProxy which returns the singleton Service.
           * An outer FactoryToService (wrapping the whole stack)
           * unwraps it.
           *
           * This outer FactoryToService also closes the service after
           * each request, but we don't want to close the singleton,
           * since it is itself a FactoryToService, so closing it
           * closes the underlying factory; thus we wrap the service
           * in a proxy which ignores the close.
           *
           * The underlying services are still closed by the inner
           * FactoryToService, and the underlying factory is still
           * closed when close is called on the outer FactoryToService.
           *
           * This is too complicated.
           */
          val service = Future.value(new ServiceProxy[Req, Rep](new FactoryToService(next)) {
            override def close(deadline: Time): Future[Unit] = Future.Done
          })
          new ServiceFactoryProxy(next) {
            override def apply(conn: ClientConnection): Future[ServiceProxy[Req, Rep]] = service
          }
        } else {
          next
        }
      }
    }
}

/**
 * Turns a [[com.twitter.finagle.ServiceFactory]] into a
 * [[com.twitter.finagle.Service]] which acquires a new service for
 * each request.
 */
class FactoryToService[Req, Rep](factory: ServiceFactory[Req, Rep])
  extends Service[Req, Rep]
{
  def apply(request: Req): Future[Rep] =
    factory().flatMap { service =>
      service(request).ensure {
        service.close()
      }
    }

  override def close(deadline: Time): Future[Unit] = factory.close(deadline)
  override def status: Status = factory.status
}

/**
 * A ServiceFactoryWrapper adds behavior to an underlying ServiceFactory.
 */
trait ServiceFactoryWrapper {
  def andThen[Req, Rep](factory: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep]
}

object ServiceFactoryWrapper {
  val identity: ServiceFactoryWrapper = new ServiceFactoryWrapper {
    def andThen[Req, Rep](factory: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = factory
  }
}
