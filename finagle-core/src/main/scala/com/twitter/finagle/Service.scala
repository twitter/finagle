package com.twitter.finagle

import java.net.SocketAddress
import com.twitter.util.Future

object Service {
  /**
   * Wrap an underlying service such that any synchronously thrown exceptions are lifted into
   * Future.exception
   */
  def rescue[Req, Rep](service: Service[Req, Rep]) = new ServiceProxy[Req, Rep](service) {
    override def apply(request: Req) = {
      try {
        service(request)
      } catch {
        case e => Future.exception(e)
      }
    }
  }
}

/**
 * A Service is an asynchronous function from Request to Future[Response]. It is the
 * basic unit of an RPC interface.
 *
 * '''Note:''' this is an abstract class (vs. a trait) to maintain java
 * compatibility, as it has implementation as well as interface.
 */
abstract class Service[-Req, +Rep] extends (Req => Future[Rep]) {
  def map[Req1](f: Req1 => Req) = new Service[Req1, Rep] {
    def apply(req1: Req1) = Service.this.apply(f(req1))
    override def release() = Service.this.release()
  }

  /**
   * This is the method to override/implement to create your own Service.
   */
  def apply(request: Req): Future[Rep]

  /**
   * Relinquishes the use of this service instance. Behavior is
   * undefined is apply() is called after resources are relinquished.
   */
  def release() = ()

  /**
   * Determines whether this service is available (can accept requests
   * with a reasonable likelihood of success).
   */
  def isAvailable: Boolean = true
}

/**
 * Information about a client, passed to a Service factory for each new
 * connection.
 */
trait ClientConnection {
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
   * Close the underlying client connection.
   */
  def close()

  /**
   * Expose a Future[Unit] that will be filled when the connection is closed
   * Useful if you want to trigger action on connection closing
   */
  def onClose: Future[Unit]
}

object ClientConnection {
  val nil: ClientConnection = new ClientConnection {
    private[this] val unconnected = 
      new SocketAddress { override def toString = "unconnected" }
    def remoteAddress = unconnected
    def localAddress = unconnected
    def close() {}
    def onClose = new com.twitter.util.Promise[Unit]
  }
}

/**
 * A simple proxy Service that forwards all calls to another Service.
 * This is is useful if you to wrap-but-modify an existing service.
 */
abstract class ServiceProxy[-Req, +Rep](val self: Service[Req, Rep])
  extends Service[Req, Rep] with Proxy
{
  def apply(request: Req) = self(request)
  override def release() = self.release()
  override def isAvailable = self.isAvailable
}

abstract class ServiceFactory[-Req, +Rep] {
  /**
   * Reserve the use of a given service instance. This pins the
   * underlying channel and the returned service has exclusive use of
   * its underlying connection. To relinquish the use of the reserved
   * Service, the user must call Service.release().
   */
  def make(): Future[Service[Req, Rep]]

  /**
   * Make a service that after dispatching a request on that service,
   * releases the service.
   */
  final def service: Service[Req, Rep] = new FactoryToService(this)

  /**
   * Close the factory and its underlying resources.
   */
  def close()

  def isAvailable: Boolean = true
}

/**
 * A simple proxy ServiceFactory that forwards all calls to another
 * ServiceFactory.  This is is useful if you to wrap-but-modify an
 * existing service factory.
 */
abstract class ServiceFactoryProxy[-Req, +Rep](val self: ServiceFactory[Req, Rep])
  extends ServiceFactory[Req, Rep] with Proxy
{
  def make() = self.make()
  def close() = self.close()
  override def isAvailable = self.isAvailable
}

class FactoryToService[Req, Rep](factory: ServiceFactory[Req, Rep])
  extends Service[Req, Rep]
{
  def apply(request: Req) = {
    factory.make() flatMap { service =>
      service(request) ensure { service.release() }
    }
  }

  override def release() = factory.close()
  override def isAvailable = factory.isAvailable
}
