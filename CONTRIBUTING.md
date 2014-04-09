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

[0]: http://twitter.github.io/effectivescala/
[1]: http://docs.scala-lang.org/style/scaladoc.html
