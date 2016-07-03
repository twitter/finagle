package com.twitter.finagle.toggle

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.twitter.util.Try
import java.net.URL
import scala.collection.{breakOut, immutable}

/**
 * [[ToggleMap ToggleMaps]] in JSON format.
 *
 * @define jsonschema The [[http://json-schema.org/ JSON Schema]] used is:
 * {{{
 * {
 *   "$schema": "http://json-schema.org/draft-04/schema#",
 *   "type": "object",
 *   "required": [
 *     "toggles"
 *   ],
 *   "properties": {
 *     "toggles": {
 *       "type": "array",
 *       "items": {
 *         "type": "object",
 *         "properties": {
 *           "id": { "type": "string" },
 *           "description": { "type": "string" },
 *           "fraction": {
 *             "type": "number",
 *             "minimum": 0.0,
 *             "maximum": 1.0,
 *             "exclusiveMinimum": false,
 *             "exclusiveMaxmimum": false
 *           },
 *           "comment": { "type": "string" }
 *         },
 *         "required": [
 *           "id",
 *           "description",
 *           "fraction"
 *         ]
 *       }
 *     }
 *   }
 * }
 * }}}
 *
 * @define example Here is an example of a JSON [[ToggleMap]] input:
 * {{{
 * {
 *   "toggles": [
 *     {
 *       "id": "com.game.of.thrones.WargingEnabled",
 *       "description": "Use warging for computing the response.",
 *       "fraction": 0.1
 *     },
 *     {
 *       "id": "com.game.of.thrones.IsWinterComing",
 *       "description": "Controls whether or not winter is coming.",
 *       "fraction": 1.0,
 *       "comment": "We've seen the white walkers, we know that winter is coming."
 *     }
 *   ]
 * }
 * }}}
 *
 * With the exception of "comment", the properties correspond to the various
 * fields on [[Toggle.Metadata]].
 *  - '''name''': Corresponds to `Toggle.Metadata.id`.
 *  - '''fraction''': Corresponds to `Toggle.Metadata.fraction` and values must be
 *    between `0.0` and `1.0`, inclusive.
 *  - '''description''': Corresponds to `Toggle.Metadata.description`.
 *  - '''comment''': For documentation purposes only and is not used in the
 *    creation of the [[ToggleMap]].
 */
object JsonToggleMap {

  private[this] val mapper: ObjectMapper =
    new ObjectMapper().registerModule(DefaultScalaModule)

  private[this] case class JsonToggle(
      @JsonProperty(required = true) id: String,
      @JsonProperty(required = true) fraction: Double,
      @JsonProperty(required = true) description: String,
      comment: Option[String])

  private[this] case class JsonToggles(
      @JsonProperty(required = true) toggles: Seq[JsonToggle]) {

    def toToggleMap: ToggleMap = {
      val metadata: immutable.Seq[Toggle.Metadata] =
        toggles.map { jsonToggle =>
          Toggle.Metadata(jsonToggle.id, jsonToggle.fraction, Some(jsonToggle.description))
        }(breakOut)

      val ids = metadata.map(_.id)
      val uniqueIds = ids.distinct
      if (ids.size != uniqueIds.size) {
        throw new IllegalArgumentException(
          s"Duplicate Toggle ids found: ${ids.mkString(",")}")
      }
      new ToggleMap.Immutable(metadata)
    }
  }

  /**
   * Attempts to parse the given JSON `String` into a [[ToggleMap]].
   *
   * $jsonschema
   *
   * $example
   */
  def parse(json: String): Try[ToggleMap] = Try {
    val jsonToggles = mapper.readValue(json, classOf[JsonToggles])
    jsonToggles.toToggleMap
  }

  /**
   * Attempts to parse the given JSON `URL` into a [[ToggleMap]].
   *
   * Useful for loading resource files via [[StandardToggleMap]].
   *
   * $jsonschema
   *
   * $example
   */
  def parse(url: URL): Try[ToggleMap] = Try {
    val jsonToggles = mapper.readValue(url, classOf[JsonToggles])
    jsonToggles.toToggleMap
  }

}
