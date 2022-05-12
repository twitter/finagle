package com.twitter.finagle.mysql.unit

import com.twitter.finagle.mysql.FailedToEncryptPasswordException
import com.twitter.finagle.mysql.PasswordUtils
import com.twitter.io.TempFile
import org.scalatest.funsuite.AnyFunSuite

class PasswordUtilsTest extends AnyFunSuite {
  val longByteArray: Array[Byte] = Array(0, 0, 0, 1, 1, 1, 0, 0, 0, 1)
  val shortByteArray: Array[Byte] = Array(1, 0, 1, 0)
  val rsaKeyPath: String = TempFile.fromResourcePath("/auth/keys/mysql_rsa_public_key.pem").getPath
  val rsaKeyString: String = """-----BEGIN PUBLIC KEY-----
                               |MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA3qvJnC27k9lYEmnRJuzs
                               |wrzzS+/REP7caX7Osy1kliu/nz50rkYHrL+qEw/X15YH+LaLv2IobuZ6JqvSHqjP
                               |WlJfHKzGwbg3msc4KRylzzbMWS2n/Hs07/GhaxsyCArGNjeMKoCqv45ffZ1s2rzI
                               |exeTVQGUNQVTqraJO6LLWtFdXsIDVmjoWJAtDiG7Y8QgGLMa90+7rdrZ7Jxrz9VD
                               |s+o/HHxOzJjsnDQ7xqHoPWA9N4ox4J4aOmrCH2o+d5Gbc1QQctIMcucQAoktzbns
                               |n6rx2D2jrDBewtiuf7uequoX+hcIpAmOhbN0oIWyFB/vk6kxgw4bh+OZuFwmo/30
                               |dQIDAQAB
                               |-----END PUBLIC KEY-----""".stripMargin

  test("xor handles the password and scramble being different lengths") {
    val longPasswordShortSalt = PasswordUtils.xor(longByteArray, shortByteArray)
    assert(longPasswordShortSalt.sameElements(Array(1, 0, 1, 1, 0, 1, 1, 0, 1, 1)))

    val shortPasswordLongSalt = PasswordUtils.xor(shortByteArray, longByteArray)
    assert(shortPasswordLongSalt.sameElements(Array(1, 0, 1, 1)))

    val sameLengthPasswordAndSalt = PasswordUtils.xor(shortByteArray, shortByteArray)
    assert(sameLengthPasswordAndSalt.sameElements(Array(0, 0, 0, 0)))

    val emptyPasswordShortSalt = PasswordUtils.xor(Array.emptyByteArray, shortByteArray)
    assert(emptyPasswordShortSalt.sameElements(Array.emptyByteArray))
  }

  test("encryptPasswordWithSha1 returns non-empty result") {
    // Hashing the same input will not result in the same output a second time, so we can only
    // verify that we get a non-empty byte array as a result of calling encryptPasswordWithSha1
    assert(PasswordUtils.encryptPasswordWithSha1("", longByteArray, 255.toShort).nonEmpty)
    assert(PasswordUtils.encryptPasswordWithSha1("password", longByteArray, 255.toShort).nonEmpty)
  }

  test("encryptPasswordWithSha256 returns non-empty result") {
    // Hashing the same input will not result in the same output a second time, so we can only
    // verify that we get a non-empty byte array as a result of calling encryptPasswordWithSha256
    assert(PasswordUtils.encryptPasswordWithSha256("", longByteArray, 255.toShort).nonEmpty)
    assert(
      PasswordUtils
        .encryptPasswordWithSha256("password", longByteArray, 255.toShort).nonEmpty)
  }

  test("getCipherTransformation returns correct transformation") {
    val oldVersionTransformation = "RSA/ECB/PKCS1Padding"
    val newVersionTransformation = "RSA/ECB/OAEPWithSHA-1AndMGF1Padding"

    assert(PasswordUtils.getCipherTransformation("3.0.10-tkdla-jda") == oldVersionTransformation)
    assert(PasswordUtils.getCipherTransformation("8.0.12ksdalk") == newVersionTransformation)

    assert(PasswordUtils.getCipherTransformation("3.18.478") == oldVersionTransformation)
    assert(PasswordUtils.getCipherTransformation("8.0.4") == oldVersionTransformation)
    assert(PasswordUtils.getCipherTransformation("8.0.5") == newVersionTransformation)
    assert(PasswordUtils.getCipherTransformation("15.0.22") == newVersionTransformation)
  }

  test("getCipherTransformation throws an error on invalid versions") {
    intercept[FailedToEncryptPasswordException] {
      PasswordUtils.getCipherTransformation("")
    }

    intercept[FailedToEncryptPasswordException] {
      PasswordUtils.getCipherTransformation("version")
    }
  }

  test("readFromPath returns correct result") {
    assert(PasswordUtils.readFromPath(rsaKeyPath) == rsaKeyString)

    intercept[FailedToEncryptPasswordException] {
      PasswordUtils.readFromPath("")
    }
  }

  test("decodeRsaPublicKey returns result with correct format and public exponent") {
    val rsaKey = PasswordUtils.decodeRsaPublicKey(rsaKeyString)
    val expectedPublicExponent = 65537
    val expectedFormat = "X.509"

    assert(rsaKey.getPublicExponent.intValue == expectedPublicExponent)
    assert(rsaKey.getFormat.contentEquals(expectedFormat))
  }

  test("decodeRsaPublicKey throws error with bad RSA key") {
    val badRsayKey = """-----BEGIN PUBLIC KEY-----
                       |00000000000000000000000000000000000000000000000000000
                       |-----END PUBLIC KEY-----""".stripMargin
    intercept[FailedToEncryptPasswordException] {
      PasswordUtils.decodeRsaPublicKey(badRsayKey)
    }
  }

  test("decodeRsaPublicKey throws an error with empty string as RSA key") {
    intercept[FailedToEncryptPasswordException] {
      PasswordUtils.decodeRsaPublicKey("")
    }
  }

  test("encryptPasswordWithRsaPublicKey returns correct result") {
    // Encrypting the same input will not result in the same output a second time, so we can only
    // verify that we get a non-empty byte array as a result of calling encryptPasswordWithRsaPublicKey
    val res1 = PasswordUtils.encryptPasswordWithRsaPublicKey(
      "password",
      rsaKeyString,
      Some(longByteArray),
      255.toShort,
      "5.7.3")
    assert(res1.nonEmpty)

    // No password
    val res2 = PasswordUtils.encryptPasswordWithRsaPublicKey(
      "",
      rsaKeyString,
      Some(longByteArray),
      255.toShort,
      "5.7.1")
    assert(res2.nonEmpty)
  }

  test("encryptPasswordWithRsaPublicKey throws an error with invalid RSA key") {
    // Empty string
    intercept[FailedToEncryptPasswordException] {
      PasswordUtils.encryptPasswordWithRsaPublicKey(
        "password",
        "",
        Some(longByteArray),
        255.toShort,
        "5.7.8")
    }
  }

  test("encryptPasswordWithRsaPublicKey throws an error with an empty array for the salt") {
    intercept[FailedToEncryptPasswordException] {
      PasswordUtils.encryptPasswordWithRsaPublicKey(
        "password",
        rsaKeyString,
        Some(Array.emptyByteArray),
        255.toShort,
        "5.7.5")
    }
  }

  test("encryptPasswordWithRsaPublicKey throws an error with no salt") {
    intercept[FailedToEncryptPasswordException] {
      PasswordUtils.encryptPasswordWithRsaPublicKey(
        "password",
        rsaKeyString,
        None,
        255.toShort,
        "5.7.6")
    }
  }

  test("encryptPasswordWithRsaPublicKey throws an error with bad version") {
    intercept[FailedToEncryptPasswordException] {
      PasswordUtils.encryptPasswordWithRsaPublicKey(
        "password",
        rsaKeyString,
        Some(longByteArray),
        255.toShort,
        "version")
    }
  }
}
