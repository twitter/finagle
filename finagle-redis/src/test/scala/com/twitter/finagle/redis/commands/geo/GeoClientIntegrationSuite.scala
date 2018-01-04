package com.twitter.finagle.redis.commands.geo

import java.lang.{Double => JDouble}

import com.twitter.conversions.time._
import com.twitter.finagle.redis.RedisClientTest
import com.twitter.finagle.redis.protocol.{GeoElement, GeoRadiusResult, GeoUnit}
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.io.Buf
import com.twitter.io.Buf.Utf8
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.{Matchers, OptionValues}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoClientIntegrationSuite extends RedisClientTest with Matchers with OptionValues {
  def await[A](a: Future[A]): A = Await.result(a, 5.seconds)

  private val sicily: Buf = "Sicily"

  private val palermo: Buf = "Palermo"
  private val palermoGeo: GeoElement = GeoElement(13.361389, 38.115556, palermo)

  private val catania: Buf = "Catania"
  private val cataniaGeo: GeoElement = GeoElement(15.087269, 37.502669, catania)

  test("Example in GEOADD command page", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(await(client.geoAdd(sicily, palermoGeo, cataniaGeo)) === 2L)
      assert(await(client.geoDist(sicily, palermo, catania)).value === 166274.1516)
      assert(await(client.geoRadius(sicily, 15.0, 37.0, 100.0, GeoUnit.Kilometer)) ===
        Seq(Some(GeoRadiusResult(catania))))
      assert(await(client.geoRadius(sicily, 15.0, 37.0, 200.0, GeoUnit.Kilometer)) ===
        Seq(Some(GeoRadiusResult(palermo)), Some(GeoRadiusResult(catania))))
    }
  }

  test("Example in GEOHASH command page", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(await(client.geoAdd(sicily, palermoGeo, cataniaGeo)) === 2L)
      assert(await(client.geoHash(sicily, palermo, catania)) ===
        Seq(Some(Utf8("sqc8b49rny0")), Some(Utf8("sqdtr74hyu0"))))
    }
  }

  def toScala(jd: JDouble): Double = Double.unbox(jd)

  test("Example in GEOPOS command page", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(await(client.geoAdd(sicily, palermoGeo, cataniaGeo)) === 2L)
      assert(await(client.geoPos(sicily, palermo, catania, "NonExisiting")) ===
        Seq(Some((13.36138933897018433, 38.11555639549629859)),
          Some(15.08726745843887329, 37.50266842333162032), None))
    }
  }

  test("Example in GEODIST command page", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(await(client.geoAdd(sicily, palermoGeo, cataniaGeo)) === 2L)
      assert(await(client.geoDist(sicily, palermo, catania)).value === 166274.1516)
      assert(await(client.geoDist(sicily, palermo, catania, Some(GeoUnit.Kilometer))).value === 166.2742)
      assert(await(client.geoDist(sicily, palermo, catania, Some(GeoUnit.Mile))).value === 103.3182)
      assert(await(client.geoDist(sicily, "foo", "bar")) === None)
    }
  }

  test("GEORADIUS command correctly performs", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(await(client.geoAdd(sicily, palermoGeo, cataniaGeo)) === 2L)

      assert(
        await(client.geoRadius(sicily, 15.0, 37.0, 200.0, GeoUnit.Kilometer, withDist = true)) ===
          Seq(Some(GeoRadiusResult(palermo, dist = Some(190.4424))),
            Some(GeoRadiusResult(catania, dist = Some(56.4413)))))

      assert(await(client.geoRadius(sicily, 15.0, 37.0, 200.0, GeoUnit.Kilometer, withCoord = true)) ===
        Seq(Some(GeoRadiusResult(palermo, coord = Some((13.36138933897018433, 38.11555639549629859)))),
        Some(GeoRadiusResult(catania, coord = Some((15.08726745843887329, 37.50266842333162032)))))
      )

      assert(await(client.geoRadius(sicily, 15.0, 37.0, 200.0, GeoUnit.Kilometer,
        withDist = true, withCoord = true)) ===
          Seq(
            Some(GeoRadiusResult(palermo, dist = Some(190.4424),
              coord = Some((13.36138933897018433, 38.11555639549629859)))),
            Some(GeoRadiusResult(catania, dist = Some(56.4413),
              coord = Some((15.08726745843887329, 37.50266842333162032))))
          ))
    }
  }

  test("GEORADIUSBYMEMBER command correctly performs", RedisTest, ClientTest) {
    withRedisClient { client =>
      val agrigento = Buf.Utf8("Agrigento")
      val agrigentoGeo: GeoElement = GeoElement(13.583333, 37.316667, agrigento)
      assert(await(client.geoAdd(sicily, agrigentoGeo, palermoGeo, cataniaGeo)) === 3L)

      assert(await(client.geoRadiusByMember(sicily, agrigento, 100.0, GeoUnit.Kilometer)) ===
        Seq(Some(GeoRadiusResult(agrigento)), Some(GeoRadiusResult(palermo))))

    }
  }
}
