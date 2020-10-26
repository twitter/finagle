package com.twitter.finagle.postgresql.machine

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationGSS
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationKerberosV5
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationSASL
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationSCMCredential
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationSSPI
import com.twitter.finagle.postgresql.FrontendMessage
import com.twitter.finagle.postgresql.Params
import com.twitter.finagle.postgresql.PgSqlInvalidMachineStateError
import com.twitter.finagle.postgresql.PgSqlPasswordRequired
import com.twitter.finagle.postgresql.PgSqlUnsupportedAuthenticationMechanism
import com.twitter.finagle.postgresql.PropertiesSpec
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.machine.StateMachine.Complete
import com.twitter.finagle.postgresql.machine.StateMachine.Respond
import com.twitter.finagle.postgresql.machine.StateMachine.Send
import com.twitter.finagle.postgresql.machine.StateMachine.Transition
import com.twitter.io.Buf
import com.twitter.util.Return
import org.specs2.matcher.MatchResult

class HandshakeMachineSpec extends MachineSpec[Response.ConnectionParameters] with PropertiesSpec {

  val checkStartup = checkResult("start is a startup message") {
    case Transition(_, Send(s)) => s must beAnInstanceOf[FrontendMessage.StartupMessage]
  }
  val checkAuthSuccess = checkResult("expects more messages") {
    // the state machine should expect more messages
    case Transition(_, action) => action must not(beAnInstanceOf[Respond[_]])
  }

  def mkMachine(username: String, password: Option[String], dbName: String): HandshakeMachine =
    HandshakeMachine(Params.Credentials(username = username, password = password), Params.Database(Some(dbName)))
  def mkMachine: HandshakeMachine = mkMachine("username", None, "database")

  "HandshakeMachine Authentication" should {
    "use the supplied parameters" in prop { (username: String, dbName: String) =>
      val machine = mkMachine(username, password = None, dbName = dbName)
      machineSpec(machine) {
        checkResult("start is a startup message") {
          case Transition(_, Send(s: FrontendMessage.StartupMessage)) =>
            s.user must_== username
            s.database must beSome(dbName)
        }
      }
    }

    "support password-less authentication" in {
      machineSpec(mkMachine)(
        checkStartup,
        receive(BackendMessage.AuthenticationOk),
        checkAuthSuccess
      )
    }

    "fails when password is required but not provided" in {
      machineSpec(mkMachine)(
        checkStartup,
        receive(BackendMessage.AuthenticationCleartextPassword),
        checkFailure("complete with failure") { ex =>
          ex must beEqualTo(PgSqlPasswordRequired)
        }
      )
    }

    def passwordAuthSpec(username: String, password: String)(f: => BackendMessage)(check: String => MatchResult[_]) =
      machineSpec(mkMachine(username, Some(password), "database"))(
        checkStartup,
        receive(f),
        checkResult("sends password") {
          case Transition(_, Send(FrontendMessage.PasswordMessage(sent))) => check(sent)
        },
        receive(BackendMessage.AuthenticationOk),
        checkAuthSuccess
      )

    "support clear text password authentication" in prop { (username: String, password: String) =>
      passwordAuthSpec(username, password)(BackendMessage.AuthenticationCleartextPassword)(_ must_== password)
    }

    def hex(input: Array[Byte]) = input.map(s => f"$s%02x").mkString
    def bytes(str: String) = str.getBytes(StandardCharsets.UTF_8)
    def md5(input: Array[Byte]*): String =
      hex(input.foldLeft(MessageDigest.getInstance("MD5")) { case (d, v) => d.update(v); d }.digest())

    "support md5 password authentication" in prop { (username: String, password: String, salt: Array[Byte]) =>
      passwordAuthSpec(username, password)(BackendMessage.AuthenticationMD5Password(Buf.ByteArray.Owned(salt))) {
        hashed =>
          val expectedHash = md5(bytes(md5(bytes(password), bytes(username))), salt)
          hashed must_== s"md5$expectedHash"
      }
    }

    fragments {
      List(
        AuthenticationGSS,
        AuthenticationKerberosV5,
        AuthenticationSCMCredential,
        AuthenticationSSPI,
        AuthenticationSASL("bogus")
      )
        .map { method =>
          s"fails with unsupported authentication method for $method" in {
            machineSpec(mkMachine("username", Some("password"), "database"))(
              checkStartup,
              receive(method),
              checkFailure("complete with failure") { ex =>
                ex must beEqualTo(PgSqlUnsupportedAuthenticationMechanism(method))
              }
            )
          }
        }
    }
  }

  "HandshakeMachine Startup" should {
    val authSuccess = checkStartup :: receive(BackendMessage.AuthenticationOk) :: checkAuthSuccess :: Nil

    "accumulate backend parameters" in prop {
      (parameters: List[BackendMessage.ParameterStatus], bkd: BackendMessage.BackendKeyData) =>
        val receiveParams = parameters.map(receive(_))
        // shuffle the BackendKeyData in he ParameterStatus messages
        val startupPhase = util.Random.shuffle(receive(bkd) :: receiveParams)

        val checks = List(
          receive(BackendMessage.ReadyForQuery(BackendMessage.NoTx)),
          checkResult("responds success") {
            case Complete(_, Some(Return(result))) =>
              result.parameters must containTheSameElementsAs(parameters)
              result.backendData must beEqualTo(bkd)
          }
        )

        machineSpec(mkMachine)(
          authSuccess ++ startupPhase ++ checks: _*
        )
    }

    "fails if missing BackendKeyData" in {
      machineSpec(mkMachine)(
        authSuccess ++ List(
          receive(BackendMessage.ReadyForQuery(BackendMessage.NoTx)),
          checkFailure("fails") { ex =>
            ex must beAnInstanceOf[PgSqlInvalidMachineStateError]
          }
        ): _*
      )
    }
  }
}
