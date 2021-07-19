# How to Contribute

We'd love to get patches from you!

## Nightly Snapshots

Snapshots are published nightly for the current version in development and are
available in the [Sonatype](https://oss.sonatype.org/) open source snapshot
repository: [https://oss.sonatype.org/content/repositories/snapshots/](https://oss.sonatype.org/content/repositories/snapshots/). 

## Building dependencies

If you want to manually build and publish the `develop` branches of Finagle's 
dependencies locally, you can use our build tool, [dodo](https://github.com/twitter/dodo).

``` bash
curl -s https://raw.githubusercontent.com/twitter/dodo/develop/bin/build | bash -s -- --no-test finagle
```

This will clone, build, and publish locally the current `-SNAPSHOT` version 
from the `develop` branch of Finagle's other Twitter open source dependencies.

It is your choice to use the published nightly snapshot versions or to build 
the snapshots locally from their respective `develop` branches via the
[dodo](https://github.com/twitter/dodo) build tool.

## Building Finagle

Finagle is built using [sbt][sbt]. When building please use the included
[`./sbt`](https://github.com/twitter/finagle/blob/develop/sbt) script which
provides a thin wrapper over [sbt][sbt] and correctly sets memory and other
settings.

If you have any questions or run into any problems, please create
an issue here, chat with us in [gitter](https://gitter.im/twitter/finagle), or email
the Finaglers [mailing list](https://groups.google.com/forum/#!forum/finaglers).

### 3rd party upgrades

We upgrade the following 3rd party libraries/tools at least once every 3 months:
* [sbt](https://github.com/sbt/sbt)
* [Netty](https://github.com/netty/netty)
* [Jackson](https://github.com/FasterXML/jackson)
* [Caffeine](https://github.com/ben-manes/caffeine)

## Workflow

The workflow that we support:

1. Fork finagle
1. Check out the `develop` branch
1. Make a feature branch (use `git checkout -b "cool-new-feature"`)
1. Make your cool new feature or bugfix on your branch
1. Write a test for your change
1. From your branch, make a pull request against `twitter/finagle/develop`
1. Work with repo maintainers to get your change reviewed
1. Wait for your change to be pulled into `twitter/finagle/develop`
1. Merge `twitter/finagle/develop` into your origin `develop`
1. Delete your feature branch

## Checklist

There are a number of things we like to see in pull requests. Depending
on the scope of your change, there may not be many to take care of, but
please scan this list and see which apply. It's okay if something is missed;
the maintainers will help out during code review.

1. Include [tests](CONTRIBUTING.md#testing).
1. Update the [changelog][changes] for new features, API breakages, runtime behavior changes,
   deprecations, and bug fixes.
1. New metrics should be documented in the [user guide][metrics].
1. All public APIs should have [Scaladoc][8].
1. When adding a constructor to an existing class or arguments to an existing
   method, in order to preserve backwards compatibility for Java users, avoid
   Scala's default arguments. Instead use explicit forwarding methods.
1. The second argument of an `@deprecated` annotation should be the current
   date, in `YYYY-MM-DD` form.

## Testing

We've standardized on using the [ScalaTest testing framework][scalatest].
Because ScalaTest has such a big surface area, we use a restricted subset of it
in our tests to keep them easy to read.  We've chosen the `assert` API, not the
`Matchers` one, and we use the [`FunSuite` mixin][funsuite], which supports
xUnit-like semantics.

We encourage our contributors to ensure Java compatibility for any new public APIs
they introduce. The easiest way to do so is to provide _Java compilation tests_
and make sure the new API is easily accessible (typing `X$.MODULE$` is not easy)
from Java. These compilation tests also provide Java users with testable examples
of the API usage. For an example of a Java compilation test see
[AddrCompilationTest.java][9].

Run tests against the updated project:
``` bash
./sbt 'project $project-name' test
```

Note that while you will see a [Travis CI][travis-ci] status message in your
pull request, this may not always be accurate, and in any case all changes will
be tested internally at Twitter before being merged. We're working to make
Travis CI more useful for development, but for now you don't need to worry if
it's failing (assuming that you are able to build and test your changes
locally).

### Property-based testing

When appropriate, use [ScalaCheck][scalacheck] to write property-based
tests for your code. This will often produce more thorough and effective
inputs for your tests. We use ScalaTest's
[ScalaCheckDrivenPropertyChecks][gendrivenprop] as the entry point for
writing these tests.

## Compatibility

We try to keep public APIs stable for the obvious reasons. Often,
compatibility can be kept by adding a forwarding method. Note that we
avoid adding default arguments because this is not a compatible change
for our Java users.  However, when the benefits outweigh the costs, we
are willing to break APIs. The break should be noted in the Breaking
API Changes section of the [changelog][changes]. Note that changes to
non-public APIs will not be called out in the [changelog][changes].

## Java

While the project is written in Scala, its public APIs should be usable from
Java. This occasionally works out naturally from the Scala interop, but more
often than not, if care is not taken Java users will have rough corners
(e.g. `SomeCompanion$.MODULE$.someMethod()` or a symbolic operator).
We take a variety of approaches to minimize this.

1. Add a "compilation" unit test, written in Java, that verifies the APIs are
   usable from Java.
1. If there is anything gnarly, we add Java adapters either by adding
   a non-symbolic method name or by adding a class that does forwarding.
1. Prefer `abstract` classes over `traits` as they are easier for Java
   developers to extend.

## Style

We generally follow [Effective Scala][es] and the [Scala Style Guide][ssg]. When
in doubt, look around the codebase and see how it's done elsewhere.

## Issues

When creating an issue please try to adhere to the following format:

    module-name: One line summary of the issue (less than 72 characters)

    ### Expected behavior

    As concisely as possible, describe the expected behavior.

    ### Actual behavior

    As concisely as possible, describe the observed behavior.

    ### Steps to reproduce the behavior

    List all relevant steps to reproduce the observed behavior.

## Pull Requests

Comments should be formatted to a width no greater than 80 columns.

Files should be exempt of trailing spaces.

We adhere to a specific format for commit messages. Please write your commit
messages along these guidelines. Please keep the line width no greater than
80 columns (You can use `fmt -n -p -w 80` to accomplish this).

    module-name: One line description of your change (less than 72 characters)

    Problem

    Explain the context and why you're making that change.  What is the
    problem you're trying to solve? In some cases there is not a problem
    and this can be thought of being the motivation for your change.

    Solution

    Describe the modifications you've done.

    Result

    What will change as a result of your pull request? Note that sometimes
    this section is unnecessary because it is self-explanatory based on
    the solution.

## Code Review

The Finagle repository on GitHub is kept in sync with an internal repository at
Twitter. For the most part this process should be transparent to Finagle users,
but it does have some implications for how pull requests are merged into the
codebase.

When you submit a pull request on GitHub, it will be reviewed by the
Finagle community (both inside and outside of Twitter), and once the changes are
approved, your commits will be brought into the internal system for additional
testing. Once the changes are merged internally, they will be pushed back to
GitHub with the next release.

This process means that the pull request will not be merged in the usual way.
Instead a member of the Finagle team will post a message in the pull request
thread when your changes have made their way back to GitHub, and the pull
request will be closed (see [this pull request][0] for an example). The changes
in the pull request will be collapsed into a single commit, but the authorship
metadata will be preserved.

Please let us know if you have any questions about this process!

## Getting Started

We've created [a `Starter` label][1] for issues that we think are likely to be
reasonably limited in scope and ready to be tackled by new contributors. Please
feel free to ask questions in the issue thread or [on the mailing list][2] if
you run into problems trying to implement a new feature or fix a bug described
in one of the starter issues.

## Documentation

We also welcome improvements to the Finagle documentation, which is maintained
in this repository and hosted on [the corresponding GitHub Pages site][3].

Finagle uses [Sphinx][4] to generate its user guide via the built-in Sphinx
support in the [sbt-site plugin][5]. You'll need to [install Sphinx][6] on
your system before you can build the site locally.

Once you've got Sphinx installed, you can make changes to the [RST][7] files in
the `doc/src/sphinx` directory and then build the site with the following
command from the `finagle` directory:

``` bash
./sbt 'project finagle-doc' makeSite
```

You can then view the site locally at `doc/target/site/index.html`.

Please note that sbt-site currently will not work with the Python 3 version of
Sphinx. It's also hard-coded to call an executable named `sphinx-build`, which
on some systems may be the name of the Python 3 version, with the Python 2
version named `sphinx-build2`. If the site build process crashes with a "Failed
to build Sphinx html documentation", this is likely to be the problem. The
simplest solution is to create a symbolic link to `sphinx-build2` named
`sphinx-build` somewhere on your path.

Please note that any additions or changes to the API must be thoroughly
described in [Scaladoc][8] comments. We will also happily consider pull
requests that improve the existing Scaladocs!

[0]: https://github.com/twitter/finagle/pull/267
[1]: https://github.com/twitter/finagle/issues?direction=desc&labels=Starter&sort=created&state=open
[2]: https://groups.google.com/d/forum/finaglers
[3]: https://twitter.github.io/finagle/
[4]: https://www.sphinx-doc.org/en/master/
[5]: https://github.com/sbt/sbt-site
[6]: https://www.sphinx-doc.org/en/master/usage/installation.html
[7]: http://docutils.sourceforge.net/rst.html
[8]: https://docs.scala-lang.org/style/scaladoc.html
[9]: https://github.com/twitter/finagle/blob/release/finagle-core/src/test/java/com/twitter/finagle/AddrCompilationTest.java
[es]: https://twitter.github.io/effectivescala/
[funsuite]: https://www.scalatest.org/getting_started_with_fun_suite
[sbt]: https://www.scala-sbt.org/
[scalatest]: https://www.scalatest.org/
[scalacheck]: https://www.scalacheck.org/
[ssg]: https://docs.scala-lang.org/style/scaladoc.html
[travis-ci]: https://travis-ci.org/twitter/finagle
[metrics]: https://twitter.github.io/finagle/guide/Metrics.html
[changes]: https://github.com/twitter/finagle/blob/develop/CHANGELOG.rst
[gendrivenprop]: https://www.scalatest.org/user_guide/generator_driven_property_checks
