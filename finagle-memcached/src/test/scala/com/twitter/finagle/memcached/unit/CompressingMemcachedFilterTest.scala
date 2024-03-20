package com.twitter.finagle.memcached.unit

import com.twitter.conversions.DurationOps._
import com.twitter.io.Buf
import org.scalatest.BeforeAndAfter
import com.twitter.util.Time
import com.twitter.util.Future
import com.twitter.finagle.memcached._
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.Service
import com.twitter.finagle.memcached.compressing.CompressionProvider
import com.twitter.finagle.memcached.compressing.scheme.CompressionScheme
import com.twitter.finagle.memcached.compressing.scheme.Lz4
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.Await
import com.twitter.util.Awaitable
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableFor1
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CompressingMemcachedFilterTest
    extends AnyFunSuite
    with MockitoSugar
    with BeforeAndAfter
    with ScalaCheckPropertyChecks {

  val TimeOut = 15.seconds
  private val useCompressionFilerToggleKey = "com.twitter.finagle.filter.CompressingMemcached"

  private def awaitResult[T](awaitable: Awaitable[T]): T = Await.result(awaitable, TimeOut)

  val alwaysCompress: Buf = Buf.ByteArray.Owned(Array.fill[Byte](1024)(0))

  val stats = new InMemoryStatsReceiver

  val compressingFilter = new CompressingMemcachedFilter(Lz4, stats)

  after {
    stats.clear()
  }

  com.twitter.finagle.toggle.flag.overrides.let(useCompressionFilerToggleKey, 1) {

    test("Add is correctly processed and compressed") {

      val requestFlag = 0
      val requestKey = Buf.Utf8("key")
      val addRequest =
        Add(key = requestKey, flags = 0, expiry = Time.Top, value = alwaysCompress)

      val factory =
        CompressionProvider(Lz4, NullStatsReceiver)

      val (compressionFlags, expectedBuf) = factory.compressor(alwaysCompress)

      val expectedFlags =
        CompressionScheme.flagsWithCompression(requestFlag, compressionFlags)

      val service = mock[Service[Command, Response]]
      when(service.apply(any[Add])).thenAnswer { request =>
        val addArg = request.getArgument[Add](0)
        Future.value(
          Values(values = Seq(
            Value(
              key = addArg.key,
              value = addArg.value,
              flags = Some(Buf.Utf8(addArg.flags.toString))))))
      }

      awaitResult(compressingFilter(addRequest, service)) match {
        case Values(values) =>
          val value = values.head
          assert(value.key === requestKey)
          assert(value.value === expectedBuf)
          assert(value.flags.exists { resultFlag =>
            Buf.Utf8.unapply(resultFlag).map(_.toInt).contains(expectedFlags)
          })
        case resp => throw new IllegalStateException(s"$resp")
      }

      assert(stats.counters(Seq("lz4", "compression", "attempted")) == 1)
      assert(stats.counters(Seq("lz4", "compression", "skipped")) == 0)
    }

    test("Set is correctly processed and compressed") {

      val requestFlag = 0
      val requestKey = Buf.Utf8("key")
      val setRequest =
        Set(key = requestKey, flags = 0, expiry = Time.Top, value = alwaysCompress)

      val factory =
        CompressionProvider(Lz4, NullStatsReceiver)

      val (compressionFlags, expectedBuf) = factory.compressor(alwaysCompress)

      val expectedFlags =
        CompressionScheme.flagsWithCompression(requestFlag, compressionFlags)

      val service = mock[Service[Command, Response]]

      when(service.apply(any[Set])).thenAnswer { request =>
        val setArg = request.getArgument[Set](0)
        Future.value(
          Values(values = Seq(
            Value(
              key = setArg.key,
              value = setArg.value,
              flags = Some(Buf.Utf8(setArg.flags.toString))))))
      }

      awaitResult(compressingFilter(setRequest, service)) match {
        case Values(values) =>
          val value = values.head
          assert(value.key === requestKey)
          assert(value.value === expectedBuf)
          assert(value.flags.exists { resultFlag =>
            Buf.Utf8.unapply(resultFlag).map(_.toInt).contains(expectedFlags)
          })
        case resp => throw new IllegalStateException(s"$resp")
      }

      assert(stats.counters(Seq("lz4", "compression", "attempted")) == 1)
      assert(!stats.counters.contains(Seq("lz4", "compression", "skipped")))
    }

    test("Check And Set is correctly processed and compressed") {

      val requestFlag = 0
      val requestKey = Buf.Utf8("key")
      val casUniqueKey = Buf.Utf8("cas")
      val casRequest =
        Cas(
          key = requestKey,
          flags = 0,
          expiry = Time.Top,
          value = alwaysCompress,
          casUnique = casUniqueKey)

      val factory =
        CompressionProvider(Lz4, NullStatsReceiver)

      val (compressionFlags, expectedBuf) = factory.compressor(alwaysCompress)

      val expectedFlags =
        CompressionScheme.flagsWithCompression(requestFlag, compressionFlags)

      val service = mock[Service[Command, Response]]

      when(service.apply(any[Cas])).thenAnswer { request =>
        val casArg = request.getArgument[Cas](0)
        Future.value(
          Values(values = Seq(
            Value(
              key = casArg.key,
              value = casArg.value,
              flags = Some(Buf.Utf8(casArg.flags.toString)),
              casUnique = Some(casUniqueKey)))))
      }

      awaitResult(compressingFilter(casRequest, service)) match {
        case Values(values) =>
          val value = values.head
          assert(value.key === requestKey)
          assert(value.value === expectedBuf)
          assert(value.flags.exists { resultFlag =>
            Buf.Utf8.unapply(resultFlag).map(_.toInt).contains(expectedFlags)
          })
          assert(value.casUnique.contains(casUniqueKey))
        case resp => throw new IllegalStateException(s"$resp")
      }

      assert(stats.counters(Seq("lz4", "compression", "attempted")) == 1)
      assert(!stats.counters.contains(Seq("lz4", "compression", "skipped")))
    }

    test("throw error for unsupported commands") {

      val requestFlag = 0
      val requestKey = Buf.Utf8("key")
      val requestValue = Buf.Utf8("Some Value")

      val storageCommands: TableFor1[StorageCommand] = Table(
        "request",
        Append(key = requestKey, flags = requestFlag, expiry = Time.Top, value = requestValue),
        Prepend(key = requestKey, flags = requestFlag, expiry = Time.Top, value = requestValue),
        Replace(key = requestKey, flags = requestFlag, expiry = Time.Top, value = requestValue),
      )

      val service = mock[Service[Command, Response]]

      forAll(storageCommands) { request: StorageCommand =>
        val error =
          intercept[UnsupportedOperationException](awaitResult(compressingFilter(request, service)))

        assert(error.getMessage == s"${request.name} is unsupported for compressing cache")
      }
    }

    test("Retrieval Command Values are correctly processed and decompressed") {

      val requestFlag = 0
      val requestKey = Buf.Utf8("key")

      val factory =
        CompressionProvider(Lz4, NullStatsReceiver)

      val (compressionFlags, compressedBuf) = factory.compressor(alwaysCompress)

      val valueFlags =
        CompressionScheme.flagsWithCompression(requestFlag, compressionFlags)

      val service = mock[Service[Command, Response]]

      val retrievalCommandTable =
        Table("retrievalCommand", Get(keys = Seq(requestKey)), Gets(keys = Seq(requestKey)))

      forAll(retrievalCommandTable) { retrievalCommand =>
        when(service.apply(retrievalCommand)).thenReturn {
          Future.value(
            Values(values = Seq(
              Value(
                key = requestKey,
                value = compressedBuf,
                flags = Some(Buf.Utf8(valueFlags.toString)),
                casUnique = None))))
        }

        awaitResult(compressingFilter(retrievalCommand, service)) match {
          case Values(values) =>
            val value = values.head
            assert(value.key === requestKey)
            assert(value.value === alwaysCompress)
            assert(value.flags.exists { resultFlag =>
              Buf.Utf8.unapply(resultFlag).map(_.toInt).contains(valueFlags)
            })
          case resp => throw new IllegalStateException(s"$resp")
        }
      }

      assert(stats.counters(Seq("lz4", "decompression", "attempted")) == 2)
    }

    test("Retrieval Command ValuesAndErrors are correctly processed and decompressed") {

      val requestFlag = 0
      val requestKey = Buf.Utf8("key")

      val factory =
        CompressionProvider(Lz4, NullStatsReceiver)

      val (compressionFlags, compressedBuf) = factory.compressor(alwaysCompress)

      val valueFlags =
        CompressionScheme.flagsWithCompression(requestFlag, compressionFlags)

      val service = mock[Service[Command, Response]]

      val retrievalCommandTable =
        Table("retrievalCommand", Get(keys = Seq(requestKey)), Gets(keys = Seq(requestKey)))

      forAll(retrievalCommandTable) { retrievalCommand =>
        when(service.apply(retrievalCommand)).thenReturn {
          Future.value(
            ValuesAndErrors(
              values = Seq(
                Value(
                  key = requestKey,
                  value = compressedBuf,
                  flags = Some(Buf.Utf8(valueFlags.toString)),
                  casUnique = None)),
              errors = Map.empty))
        }

        awaitResult(compressingFilter(retrievalCommand, service)) match {
          case ValuesAndErrors(values, _) =>
            val value = values.head
            assert(value.key === requestKey)
            assert(value.value === alwaysCompress)
            assert(value.flags.exists { resultFlag =>
              Buf.Utf8.unapply(resultFlag).map(_.toInt).contains(valueFlags)
            })
          case resp => throw new IllegalStateException(s"$resp")
        }
      }

      assert(stats.counters(Seq("lz4", "decompression", "attempted")) == 2)
    }

    test("Retrieval Command ValuesAndErrors stores errors correctly") {
      // Insert data with the wrong flags, so that it fails when it tries to
      // decompress the data.
      val wrongFlags = 12345678
      val requestKey = Buf.Utf8("key")

      val service = mock[Service[Command, Response]]

      val retrievalCommandTable =
        Table("retrievalCommand", Get(keys = Seq(requestKey)), Gets(keys = Seq(requestKey)))

      forAll(retrievalCommandTable) { retrievalCommand =>
        when(service.apply(retrievalCommand)).thenReturn {
          Future.value(
            ValuesAndErrors(
              values = Seq(
                Value(
                  key = requestKey,
                  value = alwaysCompress,
                  flags = Some(Buf.Utf8(wrongFlags.toString)),
                  casUnique = None)),
              errors = Map.empty))
        }

        awaitResult(compressingFilter(retrievalCommand, service)) match {
          case ValuesAndErrors(_, errors) =>
            assert(errors.size == 1)
            assert(errors.contains(requestKey))
          case resp => throw new IllegalStateException(s"$resp")
        }
      }
    }
  }
}
