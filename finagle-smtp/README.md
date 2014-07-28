# finagle-smtp

A minimum implementation of SMTP client for finagle according to [`RFC5321`](http://tools.ietf.org/search/rfc5321). 
Supports sending plain text emails to one or more mailing addresses.

## Usage

### Sending independent SMTP commands

The object for instantiating an SMTP client capable of sending any command defined in *RFC5321* is `Smtp`. For services created with it the request type is `Request`.
Command classes are described in `Request.scala`. Replies are differentiated by groups, which are described in `reply/ReplyGroups.scala`.
The concrete reply types are case classes described in `reply/SmtpReplies.scala`.

This allows flexible error handling:

```scala
val res = service(command) onFailure {
  // An error group
  case ex: SyntaxErrorReply => println("Syntax error: ", ex.info)

  // A concrete reply
  case ProcessingError(info) => println("Error processing request: ", info)

  // Default
  case _ => println("Error!")
}

// Or, another way:

res handle {
  ...
}
```

#### Greeting and session

Default SMTP client only connects to the server and receives its greeting, but does not send greeting to it,
as some commands may be executed without that. If the greeting is incorrect, the service is closed.
Upon closing the service a quit command is sent automatically, if not sent earlier.

### Sending an email

The object for instantiating a client capable of sending a simple email is `SmtpSimple`.
For services created with it the request type is `EmailMessage`, described in `EmailMessage.scala`.

You can create an email using `EmailBuilder` class described in `EmailBuilder.scala`:

```scala
    val email = EmailBuilder()
                .sender("from@from.com")
                .to("first@to.com", "second@to.com")
                .subject("test")
                .bodyLines("first line", "second line") //body is a sequence of lines
                .build
```

Applying the service on the email returns `Future.Done` in case of a successful operation or the first encountered error.

#### Greeting and session

Upon the connection the client receives server greeting, as in `Smtp`.
In the beginning of the session an EHLO request is sent automatically to identify the client.
In case of error session state is reset before every next try.

### Example

The example of sending email to a local SMTP server with SmtpSimple and handling errors can be seen in [`Example.scala`](https://github.com/suncelesta/finagle/blob/finagle-smtp/finagle-example/src/main/scala/com/twitter/finagle/example/smtp/Example.scala).
