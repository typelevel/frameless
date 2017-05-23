#!/bin/bash

set -eux

sbt copyReadme tut

gitbook="node_modules/gitbook-cli/bin/gitbook.js"

if ! test -e $gitbook; then
  npm install gitbook
  npm install gitbook-cli
fi

$gitbook build docs/target/tut docs/book

mv docs/book/* .

exit 0
