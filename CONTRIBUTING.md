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

We've standardized on using the ScalaTest testing framework. A lot of our older
tests are still on Specs, which we are slowly converting to ScalaTest.  If you
are looking for an easy way to start contributing, finding a few Specs tests to
move over to ScalaTest is a great way to get started!

Because ScalaTest has such a big surface area, we use a restricted subset of it
in our tests to keep them easy to read.  We use the "assert" api, and not
the "matchers" one, and we use the FunSuite mixin, which supports xUnit-like
semantics.

## Style

We generally follow [Effective Scala][0], and the [Scala Style Guide][1].  When
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

## Documentation

Finagle uses [Sphinx][2] to generate its user guide via the built-in Sphinx
support in the [sbt-site plugin][3]. You'll need to [install Sphinx][4] on your
system before you can build the site locally.

Once you've got Sphinx installed, you can make changes to the [RST][5] files in
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

[0]: http://twitter.github.io/effectivescala/
[1]: http://docs.scala-lang.org/style/scaladoc.html
[2]: http://sphinx-doc.org/
[3]: https://github.com/sbt/sbt-site
[4]: http://sphinx-doc.org/install.html
[5]: http://docutils.sourceforge.net/rst.html
