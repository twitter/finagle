package com.twitter.finagle.zookeeper

import org.specs.SpecificationWithJUnit
import com.twitter.common.zookeeper.ServerSetImpl
import com.twitter.thrift.Status._
import scala.collection.JavaConverters._
import java.net.InetSocketAddress

class ZkGroupSpec extends SpecificationWithJUnit {
  "ZkGroup" should {
    val inst = new ZkInstance
    import inst._
    doBefore(start())
    doAfter(stop())

    "represent the underlying ServerSet" in {
      val serverSet = new ServerSetImpl(zookeeperClient, "/foo/bar/baz")
      val clust = new ZkGroup(serverSet, "/foo/bar/baz")
      clust() must beEmpty

      val status8080 = serverSet.join(
        new InetSocketAddress(8080),
        Map[String, InetSocketAddress]().asJava, ALIVE)

      clust() must eventually(haveSize(1))
      val ep = clust().head.getServiceEndpoint
      ep.getHost must be_==("0.0.0.0")
      ep.getPort must be_==(8080)

      clust() must be_==(clust())
      val snap = clust()

      val status8081 = serverSet.join(
        new InetSocketAddress(8081),
        Map[String, InetSocketAddress]().asJava, ALIVE)

      clust() must eventually(haveSize(2))
      (clust() &~ snap).toSeq must beLike {
        case Seq(fst) => fst.getServiceEndpoint.getPort must be_==(8081)
      }
    }
  }

  "ZkResolver" should {
    val inst = new ZkInstance
    import inst._
    doBefore(start())
    doAfter(stop())
    val res = new ZkResolver

    "resolve ALIVE endpoints" in {
      val clust = res("localhost:%d!/foo/bar/baz".format(zookeeperAddress.getPort))
      clust() must beEmpty
      val inetClust = clust collect { case ia: InetSocketAddress => ia }
      inetClust() must be(inetClust())
      val serverSet = new ServerSetImpl(zookeeperClient, "/foo/bar/baz")
      val addr = new InetSocketAddress("127.0.0.1", 8080)
      val blahAddr = new InetSocketAddress("10.0.0.1", 80)
      val status8080 = serverSet.join(
        addr,
        Map[String, InetSocketAddress]("blah" -> blahAddr).asJava, ALIVE)
      inetClust() must eventually(haveSize(1))
      inetClust().toSeq must beLike {
        case Seq(addr1) => addr == addr1
      }
      status8080.leave()
      inetClust() must eventually(beEmpty)
      serverSet.join(
        addr,
        Map[String, InetSocketAddress]("blah" -> blahAddr).asJava, ALIVE)
      inetClust() must eventually(haveSize(1))

      val blahClust = res("localhost:%d!/foo/bar/baz!blah".format(zookeeperAddress.getPort))
      blahClust() must eventually(haveSize(1))
      blahClust() must be(blahClust())
      blahClust().toSeq must beLike {
        case Seq(addr1) => addr1 == blahAddr
      }
    }
  }
}
