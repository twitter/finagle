/** Copyright 2010 Twitter, Inc. */
// Initially lifted from Hosebird, then hacked to shit
package com.twitter.finagle.hosebird

import java.io.{StringReader, IOException}

import org.codehaus.jackson._
import scala.collection.mutable.ListBuffer

class StatusParserException extends Exception
class StatusParserMissingException extends Exception

/**
 * Pulls a status in JSON markup apart.
 */
class StatusParserJackson {
  val factory = new JsonFactory()
  var entityTextBuffer = new StringBuilder()
  var nestingLevel = 0
  var reader:JsonParser = null
  var token:JsonToken = null
  var fieldName:String = null
  var state: State = null

  def reset() = {
    state = new State
    entityTextBuffer = new StringBuilder
    nestingLevel = 0
    reader = null
    token = null
    fieldName = null
  }

  def parseForState(status: String): State = {
    parse(status)
    state
  }

  def parse(status: String): Int = {
    try {
      reset()
      reader = factory.createJsonParser(status)
      next()
      // have to start with an object
      if (token != JsonToken.START_OBJECT) {
        throw new StatusParserException()
      }
      next()
      if (token != JsonToken.FIELD_NAME) {
        throw new StatusParserException()
      }
      state.inReplyToUserIdOpt = Some(0L)
      state.retweetIdOpt = Some(0L)
      state.retweetUserIdOpt = Some(0L)
      if (fieldName == "delete") {
        state.kind = CachedMessage.KIND_STATUS_DELETE
        readDelete()
      } else if (fieldName == "limit") {
        state.kind = CachedMessage.KIND_LIMIT
        readLimit()
      } else if (fieldName == "scrub_geo") {
        state.kind = CachedMessage.KIND_SCRUB_GEO
        readScrubGeo()
      } else {
        // can't check for a specific field here, as we don't
        // want to depend on the ordering of JSON fields
        state.kind = CachedMessage.KIND_STATUS
        state.inReplyToUserIdOpt = Some(-1L)
        state.retweetIdOpt = Some(-1L)
        state.retweetUserIdOpt = Some(-1L)
        readStatus()
      }
      if (nestingLevel != 0) {
        println("Achtung: after parsing with Jackson we're at nesting level %d for status %s. This is probably a problem.")
      }
      state.kind
    } catch {
      case e: IOException => {
        throw new StatusParserException()
      }
      case e: JsonParseException => {
        throw new StatusParserException()
      }
    }
  }

  def next() = {
    token = reader.nextToken()
    // Json equivalent of end document
    if (token == null || token == JsonToken.END_OBJECT) {
      nestingLevel -= 1
    } else if (token == JsonToken.START_OBJECT) {
      nestingLevel += 1
    } else if (token == JsonToken.FIELD_NAME) {
      fieldName = reader.getCurrentName
    }
  }

  def readToObject() = {
    next()
    while (token != JsonToken.START_OBJECT && token != JsonToken.VALUE_NULL) {
      next()
    }
  }

  def readObject(fn: => Unit):Unit = {
    readToObject()
    if (reader.getCurrentToken != JsonToken.VALUE_NULL) {
      readObject(nestingLevel)(fn)
    }
  }

  def readObject(rootLevel: Int)(fn: => Unit):Unit = {
    if (nestingLevel < rootLevel) {
      return
    } else if (token == JsonToken.FIELD_NAME) {
      if (nestingLevel <= rootLevel) {
        fn
      }
    }
    next()
    readObject(rootLevel) (fn)
  }

  def readArray(fn: => Any):Unit = {
    if (token != JsonToken.VALUE_NULL) {
      readArray(nestingLevel)(fn)
    }
  }

  def readArray(rootLevel: Int)(fn: => Unit):Unit = {
    if (nestingLevel == rootLevel && token == JsonToken.END_ARRAY) {
      return
    } else if (token == JsonToken.FIELD_NAME) {
      fn
    }
    next()
    readArray(rootLevel) (fn)
  }

  def readField() = {
    next()
    if (!token.isScalarValue) {
      throw new StatusParserException()
    }
    reader.getText()
  }

  def readLongField() = {
    next()
    if (!token.isNumeric) {
      throw new StatusParserException()
    }
    reader.getLongValue()
  }

  def readDelete() = {
    readObject {
      if (fieldName == "status") {
        readObject {
          if (fieldName == "user_id") {
            state.userIdOpt = Some(readLongField())
          } else if (fieldName == "id") {
            state.idOpt = Some(readLongField())
          }
        }
      }
    }
    // take care of object close
    next()
  }

  def readLimit() = {
    readObject {
      if (fieldName == "track") {
        state.limitTrackOpt = Some(readLongField())
      }
    }
    // take care of object close
    next()
  }

  def readScrubGeo() = {
    readObject {
      if (fieldName == "user_id") {
        state.userIdOpt = Some(readLongField())
      }
      if (fieldName == "up_to_status_id") {
        state.idOpt = Some(readLongField())
      }
    }
    // take care of object close
    next()
  }

  def readStatus() = {
    // status is special, because we've already read one of its elements
    readObject(nestingLevel) {
      if (fieldName == "geo") {
        readGeo()
      } else if (fieldName == "place") {
        readPlace()
      } else if (fieldName == "retweeted_status") {
        readRetweet()
      } else if (fieldName == "user") {
        readUser(false)
      } else if (fieldName == "annotations") {
        readAnnotations()
      } else if (fieldName == "id") {
        state.idOpt = state.idOpt match {
          case None => Some(readLongField())
          case _ => state.idOpt
        }
      } else if (fieldName == "new_id") {
        state.idOpt = Some(readLongField())
      } else if (fieldName == "source") {
        state.sourceOpt = Some(readField())
      } else if (fieldName == "created_at") {
        state.createdAtOpt = Some(readField())
      } else if (fieldName == "text") {
        state.textOpt = Some(readField())
      } else if (fieldName == "in_reply_to_user_id") {
        val token = reader.nextToken
        if (token != JsonToken.VALUE_NULL) {
          val text = reader.getText()
          state.inReplyToUserIdOpt = Some(java.lang.Long.parseLong(text))
        }
      } else if (fieldName == "entities") {
        readEntities()
      }
    }
  }

  def readRetweet() = {
    readObject {
      if (fieldName == "user") {
        readUser(true)
      } else if (fieldName == "id") {
        state.retweetIdOpt = Some(readLongField())
      } else if (fieldName == "entities") {
        readEntities()
      }
    }
  }

  def readUser(isRetweet: Boolean) = {
    readObject {
      if (fieldName == "id") {
        if (isRetweet) {
          state.retweetUserIdOpt = Some(readLongField())
        } else {
          state.userIdOpt = Some(readLongField())
        }
      }
    }
  }

  def readGeo() = {
    // we'll get called on the field name token.
    // bump to next to make sure it's not null
    readObject {
      if (fieldName == "coordinates") {
        try {
          // start array
          reader.nextToken
          // latitude
          reader.nextToken
          val latitude = reader.getDoubleValue
          // longitude
          reader.nextToken
          val longitude = reader.getDoubleValue
          state.geoOpt = Some(Geo(latitude, longitude))
        } catch {
          case e: NumberFormatException => throw new StatusParserMissingException()
        }
      }
    }
  }

  def readPlace() = {
    // we'll get called here on the field name token.
    // bump to next to make sure it's not null
    readObject {
      if (fieldName == "bounding_box") {
        readObject {
          if (fieldName == "coordinates") {
            var minX = Double.MaxValue
            var minY = Double.MaxValue
            var maxX = Double.MinValue
            var maxY = Double.MinValue
            // two start arrays
            reader.nextToken
            reader.nextToken
            // 4 tuples
            val points = for(i <- 0 to 3) {
              // start array
              reader.nextToken
              // longitude
              reader.nextToken
              val x = reader.getDoubleValue
              reader.nextToken
              val y = reader.getDoubleValue
              // end array
              reader.nextToken
              if (x < minX) minX = x
              if (y < minY) minY = y
              if (x > maxX) maxX = x
              if (y > maxY) maxY = y
            }
            // two end arrays
            reader.nextToken
            reader.nextToken
            state.placeOpt = Some(Place(minX, minY, maxX, maxY))
          }
        }
      }
    }
  }

  def readAnnotations() = {
    // we get called when the token is on fieldName.  Bump to next to see if we're null
    next()
    if (token != JsonToken.VALUE_NULL) {
      // token is array start here
      // FIXME: pull these ListBuffers up and reuse them.
      var parsedAnnotations = new ListBuffer[Annotation]()
      // should get an object start here, and an object start for
      // each subsequent annotation definition
      token = reader.nextToken
      while (token == JsonToken.START_OBJECT) {
        reader.nextToken()
        val typeName = reader.getCurrentName()
        if (reader.nextToken == JsonToken.START_OBJECT) {
          var attributes = new ListBuffer[AnnotationAttribute]()
          token = reader.nextToken
          while (token == JsonToken.FIELD_NAME) {
            val attrName = reader.getCurrentName()
            reader.nextToken
            val attrValue = reader.getText()
            attributes += AnnotationAttribute(attrName, attrValue)
            token = reader.nextToken
          }
          // get the object end for the entire annotation
          reader.nextToken
          parsedAnnotations += Annotation(typeName, attributes)
        }
        token = reader.nextToken
      }
      state.annotationsOpt = Some(parsedAnnotations)
    }
  }

  def readEntities():Unit = {
    readObject {
      if (fieldName == "urls") {
        readArray {
          if (fieldName == "url" || fieldName == "expanded_url" || fieldName == "display_url") {
            entityTextBuffer.append(readField()).append(" ")
          }
        }
      } else if (fieldName == "hashtags") {
        readArray {
          if (fieldName == "text") {
            entityTextBuffer.append(readField()).append(" ")
          }
        }
      }
    }
  }

  def geo(): Option[Geo] = state.geoOpt

  def place(): Option[Place] = state.placeOpt

  def inReplyToUserId(): Long = {
    state.inReplyToUserIdOpt match {
      case Some(l) => l
      case None => -1L
    }
  }

  def statusId(): Long = {
    state.idOpt match {
      case Some(l) => l
      case None => throw new StatusParserMissingException()
    }
  }

  def source(): String = {
    state.sourceOpt match {
      case Some(s) => s
      case None => throw new StatusParserMissingException()
    }
  }

  def createdAt(): String = {
    state.createdAtOpt match {
      case Some(s) => s
      case None => throw new StatusParserMissingException()
    }
  }

  def text(): String = {
    state.textOpt match {
      case Some(s) => {
        entityText match {
          case Some(t) => s + " " + t
          case _ => s
        }
      }
      case None => throw new StatusParserMissingException()
    }
  }

  def retweetId(): Long = {
    state.retweetIdOpt match {
      case Some(l) => l
      case _ => -1L
    }
  }

  def retweetUserId(): Long = {
    state.retweetUserIdOpt match {
      case Some(l) => l
      case _ => -1L
    }
  }

  def userId(): Long = {
    state.userIdOpt match {
      case Some(l) => l
      case _ => throw new StatusParserMissingException()
    }
  }

  def limitTrack(): Long = {
    state.limitTrackOpt match {
      case Some(l) => l
      case _ => -1
    }
  }

  def annotations(): Option[Seq[Annotation]] = state.annotationsOpt

  def entityText(): Option[String] = {
    val text = entityTextBuffer.toString.trim
    if (text.size > 0) {
      Some(text)
    } else {
      None
    }
  }

  def createLimit(track: Long): String = {
    val sb = new StringBuffer(100)
    sb.append("{\"limit\":{\"track\":")
    sb.append(track)
    sb.append("}}")
    sb.toString
  }

  def createScrubGeo(userId: Long, statusId: Long): String = {
    val sb = new StringBuffer(100)
    sb.append("{\"scrub_geo\":{\"user_id\":")
    sb.append(userId)
    sb.append(",\"up_to_status_id\":")
    sb.append(statusId)
    sb.append("}}")
    sb.toString
  }
}

