# How to Contribute

We'd love to get patches from you!

## Building dependencies

We are not currently publishing snapshots for Finagle's dependencies, which
means that it may be necessary to publish the `develop` branches of these
libraries locally in order to work on Finagle's `develop` branch. To do so
you can run `./bin/travisci` script and pass it a `TRAVIS_SCALA_VERSION`
environment variable. For example, the following command locally publishes
all the Finagle dependencies built for Scala 2.11.7.

```
TRAVIS_SCALA_VERSION=2.11.7 ./bin/travisci
```

We are planning to begin publishing snapshots soon, which will make these steps
unnecessary. If you have any questions or run into any problems, please create
an issue here, tweet at us at [@finagle](https://twitter.com/finagle), or email
the Finaglers mailing list.

## Workflow

The workflow that we support:

1.  Fork finagle
2.  Check out the `develop` branch
3.  Make a feature branch (use `git checkout -b "cool-new-feature"`)
4.  Make your cool new feature or bugfix on your branch
5.  Write a test for your change
6.  From your branch, make a pull request against `twitter/finagle/develop`
7.  Work with repo maintainers to get your change reviewed
8.  Wait for your change to be pulled into `twitter/finagle/develop`
9.  Merge `twitter/finagle/develop` into your origin `develop`
10.  Delete your feature branch

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

Note that while you will see a [Travis CI][travis-ci] status message in your
pull request, this may not always be accurate, and in any case all changes will
be tested internally at Twitter before being merged. We're working to make
Travis CI more useful for development, but for now you don't need to worry if
it's failing (assuming that you are able to build and test your changes
locally).

## Style

We generally follow [Effective Scala][es] and the [Scala Style Guide][ssg]. When
in doubt, look around the codebase and see how it's done elsewhere.

Comments should be formatted to a width no greater than 80 columns.

Files should be exempt of trailing spaces.

We adhere to a specific format for commit messages. Please write your commit
messages along these guidelines:
    One line description of your change (less than 72 characters)

    Problem

    Explain here the context, and why you're making that change.
    What is the problem you're trying to solve?

    Solution

    Describe the modifications you've done.

    Result

    After your change, what will change?

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
command:

``` bash
./sbt 'project finagle-doc' make-site
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
described in [ScalaDoc][8] comments. We will also happily consider pull
requests that improve the existing ScalaDocs!

[0]: https://github.com/twitter/finagle/pull/267
[1]: https://github.com/twitter/finagle/issues?direction=desc&labels=Starter&sort=created&state=open
[2]: https://groups.google.com/d/forum/finaglers
[3]: https://twitter.github.io/finagle/
[4]: http://sphinx-doc.org/
[5]: https://github.com/sbt/sbt-site
[6]: http://sphinx-doc.org/install.html
[7]: http://docutils.sourceforge.net/rst.html
[8]: http://docs.scala-lang.org/style/scaladoc.html
[9]: https://github.com/twitter/finagle/blob/master/finagle-core/src/test/java/com/twitter/finagle/AddrCompilationTest.java
[es]: https://twitter.github.io/effectivescala/
[funsuite]: http://www.scalatest.org/getting_started_with_fun_suite
[ostrich]: https://github.com/twitter/ostrich
[scalatest]: http://www.scalatest.org/
[ssg]: http://docs.scala-lang.org/style/scaladoc.html
[travis-ci]: https://travis-ci.org/twitter/finagle
[util]: https://github.com/twitter/util
