# How to Contribute

We'd love to get patches from you!

## Workflow

The workflow that we support:

1.  Fork finagle
2.  Make a feature branch
3.  Make your cool new feature or bugfix on your branch
4.  Write a test for your change
5.  From your branch, make a pull request against twitter/finagle/master
6.  Work with repo maintainers to get merged in
7.  Wait for your change to be pulled into twitter/finagle/master
8.  Merge twitter/finagle/master into your origin master
9.  Delete your feature branch

## Testing

We've standardized on using the [ScalaTest testing framework][0]. Some of our
older tests are still on Specs, and we are slowly converting these to ScalaTest.
If you are looking for an easy way to get involved, finding a few Specs tests to
move over to ScalaTest is a great way to get started!

Because ScalaTest has such a big surface area, we use a restricted subset of it
in our tests to keep them easy to read.  We use the `assert` api, and not the
`Matchers` one, and we use the [`FunSuite` mixin][1], which supports xUnit-like
semantics.

## Style

We generally follow [Effective Scala][2], and the [Scala Style Guide][3].  When
in doubt, look around the codebase and see how it's done elsewhere.

Comments should be formatted to a width no greater than 80 columns.

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
request will be closed (see [this pull request][4] for an example). The changes
in the pull request will be collapsed into a single commit, but the authorship
metadata will be preserved.

Please let us know if you have any questions about this process!

## Getting Started

Migrating a few Specs tests to ScalaTest is a great way to get started
contributing to Finagle. [This pull request][5] demonstrates what this process
has looked like for the Finagle core and a couple of subprojects, but there are
many other subprojects that still need to be migrated (`git grep 'org\.specs'`
is one quick way to get a list of remaining Specs tests).

We've also created [a `Starter` label][6] for issues that we think are likely to
be reasonably limited in scope and ready to be tackled by new contributors.
Please feel free to ask questions in the issue thread or
[on the mailing list][7] if you run into problems trying to implement a new
feature or fix a bug described in one of the starter issues.

## Documentation

We also welcome improvements to the Finagle documentation, which is maintained
in this repository and hosted on [the corresponding GitHub Pages site][8].

Finagle uses [Sphinx][9] to generate its user guide via the built-in Sphinx
support in the [sbt-site plugin][10]. You'll need to [install Sphinx][11] on
your system before you can build the site locally.

Once you've got Sphinx installed, you can make changes to the [RST][12] files in
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
described in [ScalaDoc][13] comments. We will also happily consider pull
requests that improve the existing ScalaDocs!

[0]: http://www.scalatest.org/
[1]: http://www.scalatest.org/getting_started_with_fun_suite
[2]: http://twitter.github.io/effectivescala/
[3]: http://docs.scala-lang.org/style/scaladoc.html
[4]: https://github.com/twitter/finagle/pull/267
[5]: https://github.com/twitter/finagle/pull/268
[6]: https://github.com/twitter/finagle/issues?direction=desc&labels=Starter&sort=created&state=open
[7]: https://groups.google.com/d/forum/finaglers
[8]: http://twitter.github.io/finagle/
[9]: http://sphinx-doc.org/
[10]: https://github.com/sbt/sbt-site
[11]: http://sphinx-doc.org/install.html
[12]: http://docutils.sourceforge.net/rst.html
[13]: http://docs.scala-lang.org/style/scaladoc.html
