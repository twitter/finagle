package com.twitter.finagle.mysql

import com.twitter.finagle.FailureFlags
import java.security.interfaces.RSAPublicKey
import java.security.KeyFactory
import java.security.MessageDigest
import java.security.spec.X509EncodedKeySpec
import java.util.Base64
import javax.crypto.Cipher
import scala.io.Source
import scala.util.control.NonFatal

/**
 * Utilities for encrypting a password for the `mysql_native_password`
 * and `caching_sha2_password` authentication methods.
 */
private object PasswordUtils {

  /**
   * This encryption method is used for the `mysql_native_password` authentication method.
   *
   * XOR(SHA1(password), SHA1("20-bytes random data from server", SHA1(SHA1(password))))
   *
   * @param password the password for the user.
   * @param salt the salt sent from the server.
   * @param charset the character set the client is using.
   */
  def encryptPasswordWithSha1(password: String, salt: Array[Byte], charset: Short): Array[Byte] = {
    val md = MessageDigest.getInstance("SHA-1")
    val hash1 = md.digest(password.getBytes(MysqlCharset(charset).displayName))
    md.reset()

    val hash2 = md.digest(hash1)
    md.reset()

    md.update(salt)
    md.update(hash2)

    val digest = md.digest()
    xor(hash1, digest)
  }

  /**
   * This encryption method is used for the `caching_sha2_password` authentication method.
   *
   * XOR(SHA256(password), SHA256(SHA256(SHA256(password)), "20-bytes random data from server"))
   *
   * @param password the password for the user.
   * @param salt the salt sent from the server.
   * @param charset the character set the client is using.
   */
  def encryptPasswordWithSha256(
    password: String,
    salt: Array[Byte],
    charset: Short
  ): Array[Byte] = {
    val md = MessageDigest.getInstance("SHA-256")
    val hash1 = md.digest(password.getBytes(MysqlCharset(charset).displayName))
    md.reset()

    val hash2 = md.digest(hash1)
    md.reset()

    md.update(hash2)
    md.update(salt)

    val hashAndSaltDigest = md.digest()
    xor(hash1, hashAndSaltDigest)
  }

  /**
   * Encrypt the password with the server's RSA public key and the nonce.
   *
   * @param password the password for the user.
   * @param rsaKey the RSA key that is either stored locally or sent to the client from the server.
   * @param nonce the nonce sent in the AuthSwitchRequest.
   * @param charset the character set the client is using.
   * @param serverVersion the version of the server to determine which cipher transformation to use.
   */
  def encryptPasswordWithRsaPublicKey(
    password: String,
    rsaKey: String,
    nonce: Option[Array[Byte]],
    charset: Short,
    serverVersion: String
  ): Array[Byte] = {
    nonce match {
      case Some(serverSalt) =>
        try {
          val passwordWithNullByte =
            addNullByteToPassword(password.getBytes(MysqlCharset(charset).displayName))
          val xorResult = xor(passwordWithNullByte, serverSalt)
          val cipher = Cipher.getInstance(getCipherTransformation(serverVersion))
          val key = decodeRsaPublicKey(rsaKey)
          cipher.init(Cipher.ENCRYPT_MODE, key)
          cipher.doFinal(xorResult)
        } catch {
          case e: FailedToEncryptPasswordException => throw e
          case NonFatal(e) => throw new FailedToEncryptPasswordException(e)
        }
      case None => throw new FailedToEncryptPasswordException("Nonce not provided by the server.")
    }
  }

  /**
   * One of the four possible code paths for `caching_sha2_password` authentication is
   * reading the locally stored server's public key and encrypting the password with that key.
   * This approach cuts down on one round trip.
   *
   * @param path The path to the locally stored public key for the server.
   */
  def readFromPath(path: String): String = {
    try {
      val bufferedSource = Source.fromFile(path)

      try {
        bufferedSource.getLines.mkString("\n")
      } catch {
        case NonFatal(e) => throw new FailedToEncryptPasswordException(e)
      } finally {
        bufferedSource.close()
      }
    } catch {
      case e: FailedToEncryptPasswordException => throw e
      case NonFatal(e) => throw new FailedToEncryptPasswordException(e)
    }
  }

  /** Helper method that returns the result of XOR(password, scramble). */
  def xor(password: Array[Byte], scramble: Array[Byte]): Array[Byte] = {
    val returnedArray = new Array[Byte](password.length)
    password.indices foreach { i =>
      // Pulled out of the MySQL Java connector:
      // https://github.com/mysql/mysql-connector-j/blob/d64b664fa93e81296a377de031b8123a67e6def2/src/main/core-impl/java/com/mysql/cj/protocol/Security.java#L60-L68
      returnedArray(i) = (password(i) ^ scramble(i % scramble.length)).toByte
    }
    returnedArray
  }

  /** Helper method to add a null byte to the end of a byte array. */
  private[mysql] def addNullByteToPassword(password: Array[Byte]): Array[Byte] = {
    password :+ 0.toByte
  }

  /** Trim the text content of the public key and decode it. */
  def decodeRsaPublicKey(rsaKey: String): RSAPublicKey = {
    try {
      val trimmedRsaKey = trimRsaKey(rsaKey)

      val certBytes = Base64.getDecoder.decode(trimmedRsaKey)
      val keySpec = new X509EncodedKeySpec(certBytes)

      val keyFactory = KeyFactory.getInstance("RSA")
      keyFactory.generatePublic(keySpec).asInstanceOf[RSAPublicKey]
    } catch {
      case NonFatal(e) => throw new FailedToEncryptPasswordException(e)
    }
  }

  /**
   * Remove the key's header and footer. Strip all new lines and carriage
   * returns as they are illegal in Base64.
   * */
  def trimRsaKey(rsaKey: String): String = {
    val endOfHeader = rsaKey.indexOf("\n") + 1
    val beginningOfFooter = rsaKey.indexOf("-----END PUBLIC KEY-----")

    // java.util.Base64 doesn't support new line or carriage returns, so remove them
    // ref: https://github.com/dsyer/spring-security-rsa/issues/11
    rsaKey.substring(endOfHeader, beginningOfFooter).replaceAll("[\n|\r]", "")
  }

  /**
   * Get the correct cipher transformation given the server version.
   * Sever versions up to and including 8.0.4 use the less secure "RSA/ECB/PKCS1Padding"
   * whereas versions 8.0.5 and greater use "RSA/ECB/OAEPWithSHA-1AndMGF1Padding".
   */
  def getCipherTransformation(version: String): String = {
    val pattern = """^(\d+)\.(\d+)\.(\d+)(.*)?$""".r
    version match {
      case pattern(major, minor, patch, _) =>
        if (major.toInt >= 8 && minor.toInt >= 0 && patch.toInt >= 5)
          "RSA/ECB/OAEPWithSHA-1AndMGF1Padding"
        else
          "RSA/ECB/PKCS1Padding"
      case _ =>
        throw new FailedToEncryptPasswordException(
          "The MySQL version does not match the expected pattern.")
    }
  }
}

/**
 * The exception that is thrown if something goes awry during the encryption process.
 * This exception has the [[FailureFlags.NonRetryable]] flag because this error is
 * thrown only in cases when the client is incorrectly configured.
 */
class FailedToEncryptPasswordException(
  message: String,
  caughtException: Throwable,
  val flags: Long)
    extends Exception(s"MySQL password failed to be encrypted. $message", caughtException)
    with FailureFlags[FailedToEncryptPasswordException] {

  def this(caughtException: Throwable) = this("", caughtException, FailureFlags.NonRetryable)

  def this(message: String) = this(message, None.orNull, FailureFlags.NonRetryable)

  override protected def copyWithFlags(flags: Long): FailedToEncryptPasswordException =
    new FailedToEncryptPasswordException(message, caughtException, flags)
}
