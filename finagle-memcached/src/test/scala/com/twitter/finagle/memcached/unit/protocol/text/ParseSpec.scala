package com.twitter.finagle.memcached.unit.protocol.text

import org.specs.Specification
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.memcached.protocol.text.client.ValueLine
import text.MemcachedCommandVocabulary

class CommandVocabularySpec extends Specification {
  "MemcachedCommandVocabulary" should {
    val parser = new MemcachedCommandVocabulary

    "needsData" in {
      parser.needsData(Seq("set", "my_key", "0", "2592000", "1")) mustEqual
        Some(1)
    }

    "parse storage commands" in {
      val buffer = "bar"
      parser.parseStorageCommand(Seq("add",     "foo", "1", "2", "3"), buffer) mustEqual Add("foo", 1, 2, buffer)
      parser.parseStorageCommand(Seq("set",     "foo", "1", "2", "3"), buffer) mustEqual Set("foo", 1, 2, buffer)
      parser.parseStorageCommand(Seq("replace", "foo", "1", "2", "3"), buffer) mustEqual Replace("foo", 1, 2, buffer)
      parser.parseStorageCommand(Seq("append",  "foo", "1", "2", "3"), buffer) mustEqual Append("foo", 1, 2, buffer)
      parser.parseStorageCommand(Seq("prepend", "foo", "1", "2", "3"), buffer) mustEqual Prepend("foo", 1, 2, buffer)
    }
  }

  "MemcachedResponseVocabulary" should {
    val parser = new MemcachedResponseVocabulary

    "needsData" in {
      parser.needsData(Seq("VALUE", "key", "0", "1")) mustEqual
        Some(1)
    }

    "parse simple responses" in {
      parser.parseResponse(Seq("STORED"))     mustEqual Stored
      parser.parseResponse(Seq("NOT_STORED")) mustEqual NotStored
      parser.parseResponse(Seq("DELETED"))    mustEqual Deleted
    }

    "parse values" in {
      val one = "1"
      val two = "2"
      val values = Seq(
        ValueLine(Seq("VALUE", "foo", "0", "1"), one),
        ValueLine(Seq("VALUE", "bar", "0", "1"), two))

      parser.parseValues(values) mustEqual
        Values(Seq(
          Value("foo", one),
          Value("bar", two)))
    }
  }
}