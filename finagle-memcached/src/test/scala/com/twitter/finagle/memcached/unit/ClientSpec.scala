package com.twitter.finagle.memcached.unit

import com.twitter.finagle.memcached._
import com.twitter.hashing.KeyHasher
import org.specs.mock.Mockito
import org.specs.Specification
import scala.collection.mutable
import _root_.java.io.{BufferedReader, InputStreamReader}


object ClientSpec extends Specification with Mockito {
  "KetamaClient" should {
    "pick the correct node" in {
      // Test from Smile's KetamaNodeLocatorSpec.scala

      // Load known good results (key, hash(?), continuum ceiling(?), IP)
      val stream = getClass.getClassLoader.getResourceAsStream("ketama_results")
      val reader = new BufferedReader(new InputStreamReader(stream))
      val expected = new mutable.ListBuffer[Array[String]]
      var line: String = null
      do {
        line = reader.readLine
        if (line != null) {
          val segments = line.split(" ")
          segments.length mustEqual 4
          expected += segments
        }
      } while (line != null)
      expected.size mustEqual 99

      // Build Ketama client
      val client = mock[Client]
      val clients = Map(
        ("10.0.1.1", 11211, 600)  -> mock[Client],
        ("10.0.1.2", 11211, 300)  -> mock[Client],
        ("10.0.1.3", 11211, 200)  -> mock[Client],
        ("10.0.1.4", 11211, 350)  -> mock[Client],
        ("10.0.1.5", 11211, 1000) -> mock[Client],
        ("10.0.1.6", 11211, 800)  -> mock[Client],
        ("10.0.1.7", 11211, 950)  -> mock[Client],
        ("10.0.1.8", 11211, 100)  -> mock[Client]
      )
      val ketamaClient = new KetamaClient(clients, KeyHasher.KETAMA)

      // Test that ketamaClient.clientOf(key) == expected IP
      val mockClientToIp = clients.map { case (k,v) => v -> k._1 }
      for (testcase <- expected) {
        val mockClient = ketamaClient.clientOf(testcase(0))
        val resultIp = mockClientToIp(mockClient)
        resultIp must be_==(testcase(3))
      }
    }
  }

  "RubyMemCacheClient" should {
    "pick the correct node" in {
      val client1 = mock[Client]
      val client2 = mock[Client]
      val client3 = mock[Client]

      val rubyMemCacheClient = new RubyMemCacheClient(Seq(client1, client2, client3))
      rubyMemCacheClient.clientOf("apple")    must be_==(client1)
      rubyMemCacheClient.clientOf("banana")   must be_==(client2)
      rubyMemCacheClient.clientOf("cow")      must be_==(client1)
      rubyMemCacheClient.clientOf("dog")      must be_==(client1)
      rubyMemCacheClient.clientOf("elephant") must be_==(client3)
    }
  }
}
