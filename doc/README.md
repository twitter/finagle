# Finagle User Guide

To generate the user guide website locally, `cd` into the `finagle`
directory and run the following commands.

    $ ./sbt finagle-doc/make-site
    $ open doc/target/site/index.html

To publish a new version of the user guide to GitHub Pages:

    1. Make sure SBT sees the current version as the release version of
       Finagle (and not the SNAPSHOT version) by running from the master branch.
    2. Execute the script `finagle/pushsite.bash`.
