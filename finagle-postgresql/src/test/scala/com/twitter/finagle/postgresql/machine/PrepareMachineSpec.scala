package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.{
  BackendMessage,
  FrontendMessage,
  PgSqlNoSuchTransition,
  PropertiesSpec,
  Response
}
import com.twitter.finagle.postgresql.BackendMessage.NoData
import com.twitter.finagle.postgresql.BackendMessage.NoTx
import com.twitter.finagle.postgresql.BackendMessage.ParameterDescription
import com.twitter.finagle.postgresql.BackendMessage.ParseComplete
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.postgresql.BackendMessage.RowDescription
import com.twitter.finagle.postgresql.FrontendMessage.Describe
import com.twitter.finagle.postgresql.FrontendMessage.DescriptionTarget
import com.twitter.finagle.postgresql.FrontendMessage.Parse
import com.twitter.finagle.postgresql.FrontendMessage.Sync
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.Types.Oid
import com.twitter.finagle.postgresql.machine.StateMachine.Complete
import com.twitter.finagle.postgresql.machine.StateMachine.NoOp
import com.twitter.finagle.postgresql.machine.StateMachine.Send
import com.twitter.finagle.postgresql.machine.StateMachine.SendSeveral
import com.twitter.finagle.postgresql.machine.StateMachine.Transition
import com.twitter.util.Return

class PrepareMachineSpec extends MachineSpec[Response.ParseComplete] with PropertiesSpec {

  def checkStartup(name: Name, query: String): StepSpec =
    checkResult("start is several messages") {
      case Transition(_, SendSeveral(msgs)) =>
        msgs.toList must beLike[List[Send[_ <: FrontendMessage]]] {
          case a :: b :: c :: Nil =>
            a must be(Send(Parse(name, query, Nil)))
            b must be(Send(Describe(name, DescriptionTarget.PreparedStatement)))
            c must be(Send(Sync))
        }
    }

  def checkNoOp(name: String): StepSpec =
    checkResult(name) {
      case Transition(_, NoOp) => succeed
    }

  def mkMachine(name: Name, q: String): PrepareMachine = new PrepareMachine(name, q)
  def mkMachine: PrepareMachine = mkMachine(Name.Unnamed, "")

  "PrepareMachine" should {
    "send multiple messages on start" in prop { (name: Name, query: String) =>
      machineSpec(mkMachine(name, query)) {
        checkStartup(name, query)
      }
    }

    def nominalSpec(
      name: Name,
      query: String,
      parametersTypes: IndexedSeq[Oid],
      describeMessage: BackendMessage
    ) =
      machineSpec(mkMachine(name, query))(
        checkStartup(name, query),
        receive(ParseComplete),
        checkNoOp("handles ParseComplete"),
        receive(ParameterDescription(parametersTypes)),
        checkNoOp("handles ParameterDescription"),
        receive(describeMessage),
        checkNoOp("handles describe message"),
        receive(ReadyForQuery(NoTx)),
        checkResult("handles ReadyForQuery") {
          case Complete(_, Some(Return(Response.ParseComplete(prepared)))) =>
            prepared.name must be(name)
            prepared.parameterTypes must be(parametersTypes)
        }
      )

    "support RowDescription describe response" in prop {
      (name: Name, query: String, parametersTypes: IndexedSeq[Oid], desc: RowDescription) =>
        nominalSpec(name, query, parametersTypes, desc)
    }

    "support NoData describe response" in prop {
      (name: Name, query: String, parametersTypes: IndexedSeq[Oid]) =>
        nominalSpec(name, query, parametersTypes, NoData)
    }

    "fail when no transition exist" in {
      val machine = mkMachine(Name.Unnamed, "bogus")
      an[PgSqlNoSuchTransition] must be thrownBy machine.receive(
        machine.Parsing,
        BackendMessage.PortalSuspended)
    }
  }
}
