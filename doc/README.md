# Finagle User Guide

To generate the user guide website locally to test OSS doc changes, run the
following commands from the `finagle` directory. Note that you'll need to
[build Finagle's dependencies with dodo][0] and [install Sphinx][1] on your
system before you can build the site locally.

    $ ./sbt finagle-doc/makeSite
    $ open doc/target/site/index.html

To publish a new version of the user guide to GitHub Pages:

    1. Make sure SBT sees the current version as the release version of
       Finagle (and not the SNAPSHOT version) by running from the master branch.
    2. Execute the script `finagle/pushsite.bash`.

[0]: https://github.com/twitter/finagle/blob/release/CONTRIBUTING.md#building-dependencies
[1]: https://www.sphinx-doc.org/en/master/usage/installation.html
