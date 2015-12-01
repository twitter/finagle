#!/bin/bash


set -e

dir=/tmp/finagle.$$
trap "rm -fr $dir" 0 1 2

echo 'making site...' 1>&2
./sbt finagle-doc/make-site >/dev/null 2>&1

echo 'making unidoc...' 1>&2
./sbt unidoc >/dev/null 2>&1

echo 'cloning...' 1>&2
git clone -b gh-pages git@github.com:twitter/finagle.git $dir >/dev/null 2>&1

savedir=$(pwd)
cd $dir
git rm -fr .
touch .nojekyll
cp $savedir/site/index.html .
cp -r $savedir/target/scala-2.11/unidoc/ docs
cp -r $savedir/doc/target/site guide
git add -f .
git diff-index --quiet HEAD || (git commit -am"site push by $(whoami)"; git push origin gh-pages:gh-pages;)
