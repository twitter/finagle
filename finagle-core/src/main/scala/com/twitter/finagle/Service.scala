package com.twitter.finagle

import java.net.InetSocketAddress
import com.twitter.finagle.service.RefcountedService
import com.twitter.util.Future
import org.jboss.netty.channel.Channel

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
    override def connected() = Service.this.connected()
    override def release() = Service.this.release()
  }

  /**
   * This is the method to override/implement to create your own Service.
   */
  def apply(request: Req): Future[Rep]

  /**
   * When a client is actually connected, and local & remote addresses are
   * known, this signal is sent to the service.
   */
  def connected() = ()

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
class ClientConnection {
  // tricky. we can't fill in the Channel till the first event call from netty.
  private[finagle] var channel: Channel = null

  /**
   * Host/port of the client. This is only available after `Service#connected`
   * has been signalled.
   */
  def remoteAddress: InetSocketAddress = channel.getRemoteAddress.asInstanceOf[InetSocketAddress]

  /**
   * Host/port of the local side of a client connection. This is only
   * available after `Service#connected` has been signalled.
   */
  def localAddress: InetSocketAddress = channel.getLocalAddress.asInstanceOf[InetSocketAddress]

  /**
   * Close the underlying client connection.
   */
  def close() {
    channel.disconnect()
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

/**
 *  A Filter acts as a decorator/transformer of a service. It may apply
 * transformations to the input and output of that service:
 *
 *           (*  MyService  *)
 * [ReqIn -> (ReqOut -> RepIn) -> RepOut]
 *
 * For example, you may have a POJO service that takes Strings and
 * parses them as Ints.  If you want to expose this as a Network
 * Service via Thrift, it is nice to isolate the protocol handling
 * from the business rules. Hence you might have a Filter that
 * converts back and forth between Thrift structs. Again, your service
 * deals with POJOs:
 *
 * [ThriftIn -> (String  ->  Int) -> ThriftOut]
 *
 */
abstract class Filter[-ReqIn, +RepOut, +ReqOut, -RepIn]
  extends ((ReqIn, Service[ReqOut, RepIn]) => Future[RepOut])
{
  /**
   * This is the method to override/implement to create your own Filter.
   *
   * @param  request  the input request type
   * @param  service  a service that takes the output request type and the input response type
   *
   */
  def apply(request: ReqIn, service: Service[ReqOut, RepIn]): Future[RepOut]

  /**
   * Chains a series of filters together:
   *
   *    myModularService = handleExcetions.andThen(thrift2Pojo.andThen(parseString))
   *
   * @param  next  another filter to follow after this one
   *
   */
  def andThen[Req2, Rep2](next: Filter[ReqOut, RepIn, Req2, Rep2]) =
    new Filter[ReqIn, RepOut, Req2, Rep2] {
      def apply(request: ReqIn, service: Service[Req2, Rep2]) = {
        Filter.this.apply(request, new Service[ReqOut, RepIn] {
          def apply(request: ReqOut): Future[RepIn] = next(request, service)
          override def connected() = service.connected()
          override def release() = service.release()
          override def isAvailable = service.isAvailable
        })
      }
    }

  /**
   * Terminates a filter chain in a service. For example,
   *
   *     myFilter.andThen(myService)
   *
   * @param  service  a service that takes the output request type and the input response type.
   *
   */
  def andThen(service: Service[ReqOut, RepIn]) = new Service[ReqIn, RepOut] {
    private[this] val refcounted = new RefcountedService(service)

    def apply(request: ReqIn) = Filter.this.apply(request, refcounted)
    override def connected() = refcounted.connected()
    override def release() = refcounted.release()
    override def isAvailable = refcounted.isAvailable
  }

  def andThen(factory: ServiceFactory[ReqOut, RepIn]): ServiceFactory[ReqIn, RepOut] =
    new ServiceFactory[ReqIn, RepOut] {
      def make() = factory.make() map { Filter.this andThen _ }
      override def close() = factory.close()
      override def isAvailable = factory.isAvailable
      override def toString = factory.toString
    }

  /**
   * Conditionally propagates requests down the filter chain. This may
   * useful if you are statically wiring together filter chains based
   * on a configuration file, for instance.
   *
   * @param  condAndFilter  a tuple of boolean and filter.
   *
   */
  def andThenIf[Req2 >: ReqOut, Rep2 <: RepIn](
    condAndFilter: (Boolean, Filter[ReqOut, RepIn, Req2, Rep2])) =
    condAndFilter match {
      case (true, filter) => andThen(filter)
      case (false, _)     => this
    }
}

abstract class SimpleFilter[Req, Rep] extends Filter[Req, Rep, Req, Rep]

object Filter {
  def identity[Req, Rep] = new SimpleFilter[Req, Rep] {
    def apply(request: Req, service: Service[Req, Rep]) = service(request)
  }
}
