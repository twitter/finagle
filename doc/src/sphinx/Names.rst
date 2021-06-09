Names and Naming in Finagle
===========================

.. _finagle_names:

Finagle uses *names* [#names]_ to identify network locations. Names must be
supplied when constructing a Finagle client through
:api:`ClientBuilder.dest <com/twitter/finagle/builder/ClientBuilder>`
or through implementations of :api:`Client
<com/twitter/finagle/Client>`.

Names are represented by the data-type :api:`Name
<com/twitter/finagle/Name>` comprising two variants:

``case class Name.Bound(va: Var[Addr])``
	Identifies a set of network locations. ``Var[Addr]`` (see :ref:`addr`)
	represents a set of changeable Internet addresses --- or host,
	port pairs --- described below.

``case class Name.Path(path: Path)``
	Represents a name denoted by a hierarchical :api:`path
	<com/twitter/finagle/Path>`, represented by a sequence of byte
	strings.

:api:`Resolver.eval <com/twitter/finagle/Resolver$>` parses strings
into `Names`. Strings of the form

::

	scheme!arg

uses the given *scheme* to interpret *arg*; they always evaluate into
instances of `Name.Bound`. For example,

::

	inet!twitter.com:80

Uses the ``inet`` resolver to interpret the address ``twitter.com:80``. `Inet`,
predictably, uses DNS for this job.

The name

::

	zk!myzkhost.mycompany.com:2181!/my/zk/path

reads a serverset_ at the path ``/my/zk/path`` on the ZooKeeper ensemble
named by ``myzkhost.mycompany.com:2181``.

When a scheme is absent, `inet` is used; thus

::

	twitter.com:8080

is equivalent to

::

	inet!twitter.com:8080


Paths
-----

Names beginning with the character ``/`` are hierarchical *paths* in the
tradition of Unix. They denote an abstract location --- i.e. *what* you
want. Path names must be bound by the current namespace in order to
identify *where* the network location(s) are.

The name

::

	/s/crawler

might denote the ``crawler`` service.

.. _dtabs:

Interpreting Paths With Delegation Tables
-----------------------------------------

A delegation table (or "dtab") defines the namespace for a Finagle
transaction. A dtab comprises an ordered list of delegations which
together define how a path is interpreted. A delegation is a rewrite
rule ``src => dest``. When a name has ``src`` as a prefix, the prefix
is replaced with ``dest``; otherwise the rule does not apply.
Delegations have the concrete syntax

::

	src	=>	dest

where ``src`` and ``dest`` are paths. As an example, the delegation

::

	/s	=>	/s#/foo/bar

rewrites the path

::

	/s/crawler

to

::

	/s#/foo/bar/crawler

Note that prefixes match on path `components`, not characters; e.g.
`/s` is a prefix of `/s/crawler`, but not of `/s#/foo/bar/crawler`.

Furthermore, prefixes may contain the wildcard character `*` to match
any component. For example

::

	/s#/*/bar	=>	/t/bah

rewrites the paths

::

	/s#/foo/bar/baz

or

::

	/s#/boo/bar/baz

to

::

	/t/bah/baz

Paths beginning with ``/$/`` are called "system paths." They are interpreted
specially by Finagle, similarly to resolver schemes. Paths of the form

::

	/$/namer/path..

uses the given :api:`Namer <com/twitter/finagle/Namer>` to interpret the
remaining path. This allows Finagle to translate paths into addresses. For
example

::

	/$/inet/localhost/8080

is bound by Finagle to the Internet address ``localhost:8080``. Similarly,

::

	/$/com.twitter.serverset/zk.local.twitter.com:2181/foo/bar

is the path describing the serverset_ ``/foo/bar`` on the ZooKeeper
ensemble ``zk.local.twitter.com:2181``.

Dtabs may contain line-oriented comments beginning with ``#``. ``#``
must be preceded by a whitespace character or delimiter such as ``;``,
``|``, or ``&``.  For example, this Dtab with commentary:

::

        # delegation for /s
        /s => /a      # prefer /a
            | ( /b    # or share traffic between /b and /c
              & /c
              );

is equivalent to this Dtab without commentary:

::

       /s => /a | (/b & /c);

We use dtabs to define how logical names (e.g. ``/s/crawler``)
translate into addresses. Because rewriting is abstracted away, we can
adapt a Finagle process to its environment by manipulating its dtab.
For example, this allows us to define ``/s/crawler`` to mean one set
of hosts when a process is running in a production setting, and
another set of hosts when developing or testing. A more complete
example follows.

With the dtab

::

	/zk#	=>	/$/com.twitter.serverset;
	/zk	=>	/zk#;
	/s##	=>	/zk/zk.local.twitter.com:2181;
	/s#	=>	/s##/prod;
	/s	=>	/s#;

the path

::

	/s/crawler

is rewritten thus:

::

	1.	/s/crawler
	2.	/s#/crawler
	3.	/s##/prod/crawler
	4.	/zk/zk.local.twitter.com:2181/prod/crawler
	5.	/zk#/zk.local.twitter.com:2181/prod/crawler
	6.	/$/com.twitter.serverset/zk.local.twitter.com:2181/prod/crawler

We've turned the path /s/crawler into the serverset_ ``/prod/crawler``
on `zk.local.twitter.com:2181`. We use the ``#`` character to denote
handlers --- the path ``/s#`` "handles" ``/s`` and so on. To
see why this indirection is necessary, consider redefining ``/s`` by
adding a prefix --- a common namespacing operation. The entry

::

	/s	=>	/s/prefix

would recurse; for example the name ``/s/crawler`` would be rewritten

::

	/s/crawler
	/s/prefix/crawler
	/s/prefix/prefix/crawler
	...

and so on. With ``/s#``, we'd instead add

::

	/s	=>	/s#/prefix

to get the desired effect

::

	/s/crawler
	/s#/prefix/crawler
	...

We can easily manipulate our Dtab to affect certain parts of the resolution.
For example, if we wanted to use staging instances of services instead of their
production ones, we'd append the delegation ``/s# => /s##/staging`` making the
Dtab

::

	/zk#  => /$/com.twitter.serverset;         (a)
	/zk   => /zk#;                             (b)
	/s##  => /zk/zk.local.twitter.com:2181;    (c)
	/s#   => /s##/prod;                        (d)
	/s    => /s#;                              (e)
	/s#   => /s##/staging;                     (f)

``/s/crawler`` would then be rewritten as follows. Each step is
labelled with the rule applied from the above Dtab.

::

	    /s/crawler
	(e) /s#/crawler
	(f) /s##/staging/crawler
	(c) /zk/zk.local.twitter.com:2181/staging/crawler
	(b) /zk#/zk.local.twitter.com:2181/staging/crawler
	(a) /$/com.twitter.serverset/zk.local.twitter.com:2181/staging/crawler

Simply adding a new delegation is sufficient. Later entries are
attempted before earlier ones; if a rewrite rooted at a delegation
fails to produce an address, rewriting resumes from the next matching
delegation.

The combined effect is a fallback mechanism --- if the
``crawler`` exists in the staging environment, it is used; otherwise
we fall back to its production definition.

In the above example, if ``/staging/crawler`` did not exist on
``zk.local.twitter.com:2181``, the search would backtrack from (a),
producing the following set of rewrites:

::

	    /s/crawler
	(e) /s#/crawler
	(f) /s##/staging/crawler
	  (c) /zk/zk.local.twitter.com:2181/staging/crawler
	  (b) /zk#/zk.local.twitter.com:2181/staging/crawler
	  (a) /$/com.twitter.serverset/zk.local.twitter.com:2181/staging/crawler
	(d) /s##/prod/crawler
	  (c) /zk/zk.local.twitter.com:2181/prod/crawler
	  (b) /zk#/zk.local.twitter.com:2181/prod/crawler
	  (a) /$/com.twitter.serverset/zk.local.twitter.com:2181/prod/crawler

We now see that delegations provide a simple and flexible means by
which to define a namespace. Its effect is similar to that of a Unix
mount table: Names stand on their own, but the minutiae of binding is
handled by the environment --- i.e. the dtab.

Delegations are passed between servers if a supported protocol is
used. Thus a server alters the interpretation of names in the context
of the *entire request graph*, allowing a server to affect downstream
behavior for the current transaction. As an example a developer might
want to replace an individual component in a distributed system with a
development version of that component. This can be done by
orchestrating the originator (for example, an HTTP frontend) to add a
delegation expressing this override.

Finagle has protocol support for delegation passing in :ref:`TTwitter
<thrift_and_scrooge>`, :ref:`Mux <mux>`, its variant :api:`ThriftMux
<com/twitter/finagle/ThriftMux$>`, and :api:`HTTP
<com/twitter/finagle/Http$>`. When these protocols are used,
delegations that are added dynamically to a request can be in effect
throughout the distributed request graph --- i.e. scope of the
namespace is a transaction. Delegations are added dynamically through
the :api:`Dtab <com/twitter/finagle/Dtab$>` API.

(This is a powerful facility that should be used with care.)

The Dtab API 
-----------

Delegations can be added or overridden dynamically through the 
:api:`Dtab <com/twitter/finagle/Dtab$>` API -- specifically by way of scoped 
delegation tables `Dtab.local` and `Dtab.limited`.

The `local` delegation is defined as the "per-request," propagated
scope. It's ideal for overrides you'd like to apply to the entire request
graph, as it applies to downstream services. 

The `limited` delegation is the "per-request," non-propagated
scope. Unlike `Dtab.local`, `Dtab.limited` applies only to the current request
and does not affect the rest of the call graph. Furthermore, when a 
`Dtab.limited` conflicts with a `Dtab.local`, only the `Dtab.local` is respected.  

The higher granularity of a `limited` delegation allows for more fine-grained 
control over fallback and failover behavior. This is ideal for large request 
graphs in which only some endpoints need to be rerouted. As more endpoints
begin to fail, the `local` granularity becomes a more useful "broad
strokes" approach to rerouting. 

To demonstrate this further, consider the following example service graph:
`ServiceA -> ServiceB -> ServiceC`

A `Dtab.local` set on a request to *ServiceA* will also be reflected in 
*ServiceB* and *ServiceC*. A `Dtab.limited` set on a request to *ServiceA*, 
however, will only exist for the scope of the request in *ServiceA*. The state 
of the `Dtab.limited` will not be visible to *ServiceB* or *ServiceC*.

Let's consider a few more examples:

1.  *ServiceA* sets a `Dtab.local` for *ServiceC* for a request, rerouting it to 
    *ServiceD*. *ServiceB* will, accordingly, propagate the `Dtab.local` and 
    re-route requests to *ServiceD*.  


2.  *ServiceA* sets a `Dtab.limited` for *ServiceC* for a request, rerouting it to
    *ServiceD*. This has no effect because *ServiceA* does not call *ServiceC* 
    directly and the limited state is not propagated to *ServiceB*.


3.  *ServiceA* sets a `Dtab.local` for *ServiceC*, rerouting it to *ServiceD*. 
    *ServiceA* also sets a `Dtab.limited` for *ServiceC*, rerouting it to *ServiceE*. 
    The behavior of example 1 will be seen again.


4.  *ServiceA* sets a `Dtab.limited` for *ServiceB* for a request, rerouting it to 
    *ServiceD*. The request to *ServiceB* will be rerouted, but the state will not
    be propagated. 

.. _addr:

Addr
----

`Name.Bound` comprises a ``Var[Addr]``, representing a dynamically
changing :api:`Addr <com/twitter/finagle/Addr>`. (:util:`Var
<com/twitter/util/Var>` implements a form of self-adjusting
computation); ``Addrs`` are in one of 3 states:

``Addr.Pending``
	The binding is still pending: perhaps because we are awaiting a DNS answer
	or Zookeeper operation completion.

``Addr.Neg``
	The binding was negative, meaning that the destination does not exist.

``Addr.Failed(cause: Throwable)``
	The binding failed with the given ``cause``.

``Addr.Bound(addrs: Set[Address])``
	The binding succeeded with the given set of addresses, representing
	concrete endpoints.

We now see that a ``Var[Addr]`` is capable of representing a moving target,
for example a dynamic serverset_.

.. _serverset: https://github.com/twitter-archive/commons/blob/master/src/java/com/twitter/common/zookeeper/ServerSet.java

.. rubric:: Footnotes
.. [#names] A `name` identities *what* you want; an
  `address` is a `location`, identifying `where` an object resides.
  `Binding` is the process that turns names into addresses.
