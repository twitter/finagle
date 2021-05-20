package com.twitter.finagle.thriftmux

import com.twitter.finagle.{ListeningServer, ThriftMux}
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.{Filter, Service, SimpleFilter}
import com.twitter.finagle.thriftmux.thriftscala.TestService
import com.twitter.finagle.thriftmux.thriftscala.TestService._
import com.twitter.scrooge.{Request, Response}
import com.twitter.util.{Await, Future}
import java.net.InetSocketAddress
import org.apache.thrift.TApplicationException
import org.scalatest.funsuite.AnyFunSuite

class TestServiceImpl extends TestService.ServicePerEndpoint {

  val query = new Service[Query.Args, Query.SuccessType] {
    def apply(args: Query.Args): Future[Query.SuccessType] = Future.value(args.x)
  }

  val question = new Service[Question.Args, Question.SuccessType] {
    def apply(args: Question.Args): Future[Question.SuccessType] = Future.value(args.y.reverse)
  }

  val inquiry = new Service[Inquiry.Args, Inquiry.SuccessType] {
    def apply(args: Inquiry.Args): Future[Inquiry.SuccessType] = Future.value(args.z.toUpperCase)
  }

}

class TestReqRepServiceImpl extends TestService.ReqRepServicePerEndpoint {

  val query = new Service[Request[Query.Args], Response[Query.SuccessType]] {
    def apply(req: Request[Query.Args]): Future[Response[Query.SuccessType]] =
      Future.value(Response(req.args.x))
  }

  val question = new Service[Request[Question.Args], Response[Question.SuccessType]] {
    def apply(req: Request[Question.Args]): Future[Response[Question.SuccessType]] =
      Future.value(Response(req.args.y.reverse))
  }

  val inquiry = new Service[Request[Inquiry.Args], Response[Inquiry.SuccessType]] {
    def apply(req: Request[Inquiry.Args]): Future[Response[Inquiry.SuccessType]] =
      Future.value(Response(req.args.z.toUpperCase))
  }

}

class TestFilter extends Filter.TypeAgnostic {

  def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] =
    new SimpleFilter[Req, Rep] {
      def apply(request: Req, service: Service[Req, Rep]): Future[Rep] =
        Future.exception(new Exception)
    }

}

class FilteredTest extends AnyFunSuite {

  private[this] def mkServer(service: ServicePerEndpoint): ListeningServer =
    ThriftMux.server.serveIface(":*", service.toThriftService)

  private[this] def mkReqRepServer(service: ReqRepServicePerEndpoint): ListeningServer =
    ThriftMux.server.serveIface(":*", service.toThriftService)

  private[this] def getPort(server: ListeningServer): Int =
    server.boundAddress.asInstanceOf[InetSocketAddress].getPort

  private[this] def mkClient(port: Int): TestService.MethodPerEndpoint =
    ThriftMux.client
      .build[TestService.MethodPerEndpoint]("localhost:" + port, "filter-test-client")

  private[this] def mkReqRepClient(port: Int): TestService.ReqRepServicePerEndpoint =
    ThriftMux.client
      .servicePerEndpoint[TestService.ReqRepServicePerEndpoint](
        "localhost:" + port,
        "filter-test-client")

  test("ServicePerEndpoint for TestService succeeds") {
    val service = new TestServiceImpl
    val server = mkServer(service)
    val client = mkClient(getPort(server))

    try {
      val queryResult = Await.result(client.query("xyz"), 2.seconds)
      assert(queryResult == "xyz")

      val questionResult = Await.result(client.question("xyz"), 2.seconds)
      assert(questionResult == "zyx")

      val inquiryResult = Await.result(client.inquiry("xyz"), 2.seconds)
      assert(inquiryResult == "XYZ")
    } finally {
      client.asClosable.close()
      server.close()
    }
  }

  test("ServicePerEndpoint.apply for TestService succeeds") {
    // We reuse the implementations here to pass to ServicePerEndpoint.apply
    val serviceImpl = new TestServiceImpl
    val service = ServicePerEndpoint(serviceImpl.query, serviceImpl.question, serviceImpl.inquiry)
    val server = mkServer(service)
    val client = mkClient(getPort(server))

    try {
      val queryResult = Await.result(client.query("xyz"), 2.seconds)
      assert(queryResult == "xyz")

      val questionResult = Await.result(client.question("xyz"), 2.seconds)
      assert(questionResult == "zyx")

      val inquiryResult = Await.result(client.inquiry("xyz"), 2.seconds)
      assert(inquiryResult == "XYZ")
    } finally {
      client.asClosable.close()
      server.close()
    }
  }

  test("ReqRepServicePerEndpoint for TestService succeeds") {
    val service = new TestReqRepServiceImpl
    val server = mkReqRepServer(service)
    val client = mkReqRepClient(getPort(server))

    try {
      val queryResult = Await.result(client.query(Request(Query.Args("xyz"))), 2.seconds)
      assert(queryResult.value == "xyz")

      val questionResult = Await.result(client.question(Request(Question.Args("xyz"))), 2.seconds)
      assert(questionResult.value == "zyx")

      val inquiryResult = Await.result(client.inquiry(Request(Inquiry.Args("xyz"))), 2.seconds)
      assert(inquiryResult.value == "XYZ")
    } finally {
      client.asClosable.close()
      server.close()
    }
  }

  test("ReqRepServicePerEndpoint.apply for TestService succeeds") {
    val serviceImpl = new TestReqRepServiceImpl
    val service =
      ReqRepServicePerEndpoint.apply(serviceImpl.query, serviceImpl.question, serviceImpl.inquiry)
    val server = mkReqRepServer(service)
    val client = mkReqRepClient(getPort(server))

    try {
      val queryResult = Await.result(client.query(Request(Query.Args("xyz"))), 2.seconds)
      assert(queryResult.value == "xyz")

      val questionResult = Await.result(client.question(Request(Question.Args("xyz"))), 2.seconds)
      assert(questionResult.value == "zyx")

      val inquiryResult = Await.result(client.inquiry(Request(Inquiry.Args("xyz"))), 2.seconds)
      assert(inquiryResult.value == "XYZ")
    } finally {
      client.asClosable.close()
      server.close()
    }
  }

  // For these four tests that add a filter to (ReqRep)ServicePerEndpoint
  // via the `filtered` method, we throw an exception in the included
  // filter and look for an exception to be thrown to ensure that the
  // filter has been properly added to each endpoint.
  test("ServicePerEndpoint filtered includes filter") {
    val serviceImpl = new TestServiceImpl
    val service = serviceImpl.filtered(new TestFilter)
    val server = mkServer(service)
    val client = mkClient(getPort(server))

    try {
      intercept[TApplicationException] {
        Await.result(client.query("xyz"), 2.seconds)
      }
      intercept[TApplicationException] {
        Await.result(client.question("xyz"), 2.seconds)
      }
      intercept[TApplicationException] {
        Await.result(client.inquiry("xyz"), 2.seconds)
      }
    } finally {
      client.asClosable.close()
      server.close()
    }
  }

  test("ServicePerEndpoint.apply filtered includes filter") {
    val serviceImpl = new TestServiceImpl
    val service = ServicePerEndpoint(serviceImpl.query, serviceImpl.question, serviceImpl.inquiry)
      .filtered(new TestFilter)
    val server = mkServer(service)
    val client = mkClient(getPort(server))

    try {
      intercept[TApplicationException] {
        Await.result(client.query("xyz"), 2.seconds)
      }
      intercept[TApplicationException] {
        Await.result(client.question("xyz"), 2.seconds)
      }
      intercept[TApplicationException] {
        Await.result(client.inquiry("xyz"), 2.seconds)
      }
    } finally {
      client.asClosable.close()
      server.close()
    }
  }

  test("ReqRepServicePerEndpoint filtered includes filter") {
    val serviceImpl = new TestReqRepServiceImpl
    val service = serviceImpl.filtered(new TestFilter)
    val server = mkReqRepServer(service)
    val client = mkReqRepClient(getPort(server))

    try {
      intercept[TApplicationException] {
        Await.result(client.query(Request(Query.Args("xyz"))), 2.seconds)
      }

      intercept[TApplicationException] {
        Await.result(client.question(Request(Question.Args("xyz"))), 2.seconds)
      }

      intercept[TApplicationException] {
        Await.result(client.inquiry(Request(Inquiry.Args("xyz"))), 2.seconds)
      }
    } finally {
      client.asClosable.close()
      server.close()
    }
  }

  test("ReqRepServicePerEndpoint.apply filtered includes filter") {
    val serviceImpl = new TestReqRepServiceImpl
    val service = ReqRepServicePerEndpoint
      .apply(serviceImpl.query, serviceImpl.question, serviceImpl.inquiry).filtered(new TestFilter)
    val server = mkReqRepServer(service)
    val client = mkReqRepClient(getPort(server))

    try {
      intercept[TApplicationException] {
        Await.result(client.query(Request(Query.Args("xyz"))), 2.seconds)
      }

      intercept[TApplicationException] {
        Await.result(client.question(Request(Question.Args("xyz"))), 2.seconds)
      }

      intercept[TApplicationException] {
        Await.result(client.inquiry(Request(Inquiry.Args("xyz"))), 2.seconds)
      }
    } finally {
      client.asClosable.close()
      server.close()
    }
  }
}
