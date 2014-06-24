# finagle-smtp

A minimum implementation of SMTP client for finagle. Supports sending plain text emails to one or more mailing addresses.

## Usage

### Sending independent SMTP commands

The object for instantiating an SMTP client capable of sending any command defined in *RFC5321* is `Smtp`. For services created with it the request type is `Request`.
Command classes are described in `Request.scala`. Replies are differentiated by groups, which are described in `reply/ReplyGroups.scala`.
The concrete reply types are case classes described in `reply/SmtpReplies.scala`.

This allows flexible error handling:

```scala
val res: Future[Unit] = send(command) onFailure {
  //catching by reply group
  case ex: SyntaxErrorReply => println("Syntax error: ", ex.info)

  //catching a concrete reply
  case ProcessingError(info) => println("Error processing request: ", info)
}
```

### Greeting and session

Default SMTP client only connects to the server and receives its greeting, but does not send greeting to it,
as some commands may be executed without that. If the greeting is incorrect, the service is closed.

### Sending an email

The object for instantiating a client capable of sending a simple email is `SmtpSimple`. For services created with it the request type is `EmailMessage`.

EmailMessage trait for composing emails is described in `Email.scala`. You can create an email by calling the factory method:

```scala
val email = EmailMessage(
  from = "from@from.com",
  to = Seq("first@to.com", "second@to.com"),
  subject = "test",
  body = Seq("test") //body is a sequence of lines
)
```

Applying the service on the email returns `Future.Done` in case of a successful operation or the first encountered error.

### Greeting and session

Upon the connection the client receives server greeting, as in `Smtp`.
For now in case of success the connection is closed, and in case of error session state is reset before every next try.

### Example

The example of sending email to a local SMTP server with SmtpSimple and handling errors can be seen in [`Example.scala`](https://github.com/suncelesta/finagle/blob/master/finagle-smtp/src/main/scala/com/twitter/finagle/Example.scala).
