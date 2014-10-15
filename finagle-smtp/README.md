# finagle-smtp

This is a minimum implementation of SMTP client for finagle according to 
[`RFC5321`][rfc]. The simplest guide to SMTP can be found, for example, [here][smtp2go].

Note: There is no API yet in this implementation for creating 
[`MIME`][mimewiki] messages, so the message should be plain US-ASCII text, or converted 
to such. There is currently no support for any other SMTP extensions, either. This 
functionality is to be added in future versions.

[rfc]: http://tools.ietf.org/search/rfc5321
[smtp2go]: http://www.smtp2go.com/articles/smtp-protocol
[mimewiki]: http://en.wikipedia.org/wiki/MIME

## Usage

### Sending an email

The object for instantiating a client capable of sending a simple email is `SmtpSimple`.
For services created with it the request type is `EmailMessage`, described in 
[`EmailMessage.scala`][EmailMessage].

You can create an email using `EmailBuilder` class described in [`EmailBuilder.scala`][EmailBuilder]:

```scala
    val email = EmailBuilder()
      .sender("from@from.com")
      .to("first@to.com", "second@to.com")
      .subject("test")
      .bodyLines("first line", "second line") //body is a sequence of lines
      .build
```

Applying the service on the email returns `Future.Done` in case of a successful operation.
In case of failure it returns the first encountered error wrapped in a `Future`.

[EmailMessage]: src/main/scala/com/twitter/finagle/smtp/EmailMessage.scala
[EmailBuilder]: src/main/scala/com/twitter/finagle/smtp/EmailBuilder.scala

#### Greeting and session

Upon the connection the client receives server greeting.
In the beginning of the session an EHLO request is sent automatically to identify the client.
The session state is reset before every subsequent try.

### Example

The example of sending email to a local SMTP server with SmtpSimple and handling errors can be seen 
in [`Example.scala`](src/main/scala/com/twitter/finagle/example/smtp/Example.scala).

### Sending independent SMTP commands

The object for instantiating an SMTP client capable of sending any command defined in *RFC5321* is `Smtp`. 

For services created with it the request type is `Request`. Command classes are described in 
[`Request.scala`][Request]. 

Replies are differentiated by groups, which are described in [`ReplyGroups.scala`][ReplyGroups].
The concrete reply types are case classes described in [`SmtpReplies.scala`][SmtpReplies].

This allows flexible error handling:

```scala
val res = service(command) onFailure {
  // An error group
  case ex: SyntaxErrorReply => log.error("Syntax error: %s", ex.info)

  // A concrete reply
  case ProcessingError(info) => log,error("Error processing request: %s", info)

  // Default
  case _ => log.error("Error!")
}

// Or, another way:

res handle {
  ...
}
```

[Request]: src/main/scala/com/twitter/finagle/smtp/Request.scala
[ReplyGroups]: src/main/scala/com/twitter/finagle/smtp/reply/ReplyGroups.scala
[SmtpReplies]: src/main/scala/com/twitter/finagle/smtp/reply/SmtpReplies.scala

#### Greeting and session

Default SMTP client only connects to the server and receives its greeting, but does not return greeting,
as some commands may be executed without it. In case of malformed greeting the service is closed.
Upon service.close() a quit command is sent automatically, if not sent earlier.
