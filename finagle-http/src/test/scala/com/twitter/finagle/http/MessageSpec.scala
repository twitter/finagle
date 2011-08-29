package com.twitter.finagle.http

import com.twitter.conversions.time._
import org.jboss.netty.buffer.ChannelBuffers
import org.specs.Specification
import org.specs.util.DataTables


object MessageSpec extends Specification with DataTables {
  "Message" should {
    "empty message" in {
      val response = Response()
      response.length        must_== 0
      response.contentString must_== ""
    }

    "headers" in {
      val response = Request()
      response.allow.toList must_== Nil
      response.allow = Method.Get :: Method.Head :: Nil
      response.allow must beSome("GET,HEAD")

      response.accept.toList must_== Nil
      response.accept = "text/plain; q=0.5" :: "text/html" :: Nil
      response.accept.toList must_== "text/plain; q=0.5" :: "text/html" :: Nil

      response.accept = "A,,c;param,;d,;"
      response.accept.toList must_== "A" :: "c;param" :: ";d" :: ";" :: Nil
      response.acceptMediaTypes.toList must_== "a" :: "c" :: Nil
    }

    "mediaType" in {
      "header"                                 | "type"             |>
      "application/json"                       ! "application/json" |
      "application/json;charset=utf-8"         ! "application/json" |
      ""                                       ! ""                 |
      ";"                                      ! ""                 |
      ";;;;;;;;;;"                             ! ""                 |
      "application/json;"                      ! "application/json" |
      "  application/json  ;  charset=utf-8  " ! "application/json" |
      "APPLICATION/JSON"                       ! "application/json" |
      { (header: String, expected: String) =>
        val request = Request()
        request.headers("Content-Type") = header
        if (!expected.isEmpty) // do this because None doesn't work in DataTables
          request.mediaType must_== Some(expected)
        else
          request.mediaType must_== None
      }

      val request = Request()
      request.mediaType must_== None
    }

    "clearContent" in {
      val response = Response()

      response.write("hello")
      response.clearContent()

      response.contentString must_== ""
      response.length        must_== 0
    }

    "contentString" in {
      val response = Response()
      response.setContent(ChannelBuffers.wrappedBuffer("hello".getBytes))
      response.contentString must_== "hello"
      response.contentString must_== "hello"
    }

    "cacheControl" in {
      val response = Response()

      response.cacheControl = 15123.milliseconds
      response.cacheControl must_== Some("max-age=15, must-revalidate")
    }

    "withInputStream" in {
      val response = Response()
      response.setContent(ChannelBuffers.wrappedBuffer("hello".getBytes))
      response.withInputStream { inputStream =>
        val bytes = new Array[Byte](5)
        inputStream.read(bytes)
        new String(bytes) must_== "hello"
      }
    }

    "withReader" in {
      val response = Response()
      response.setContent(ChannelBuffers.wrappedBuffer("hello".getBytes))
      response.withReader { reader =>
        val bytes = new Array[Char](5)
        reader.read(bytes)
        new String(bytes) must_== "hello"
      }
    }

    "write(String)" in {
      val response = Response()
      response.write("hello")
      response.length        must_== 5
      response.contentString must_== "hello"
    }

    "write(String), multiple writes" in {
      val response = Response()
      response.write("h")
      response.write("e")
      response.write("l")
      response.write("l")
      response.write("o")
      response.contentString must_== "hello"
      response.length        must_== 5
    }

    "withOutputStream" in {
      val response = Response()
      response.withOutputStream { outputStream =>
        outputStream.write("hello".getBytes)
      }

      response.contentString must_== "hello"
      response.length        must_== 5
    }

    "withOutputStream, multiple writes" in {
      val response = Response()
      response.write("h")
      response.withOutputStream { outputStream =>
        outputStream.write("e".getBytes)
        outputStream.write("ll".getBytes)
      }
      response.write("o")

      response.contentString must_== "hello"
      response.length        must_== 5
    }

    "withWriter" in {
      val response = Response()
      response.withWriter { writer =>
        writer.write("hello")
      }

      response.contentString must_== "hello"
      response.length        must_== 5
    }

    "withWriter, multiple writes" in {
      val response = Response()
      response.write("h")
      response.withWriter { writer =>
        writer.write("e")
        writer.write("ll")
      }
      response.write("o")

      response.contentString must_== "hello"
      response.length        must_== 5
    }
  }
}
