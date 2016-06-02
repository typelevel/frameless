#!/bin/bash

set -eux

git checkout gh-pages

git checkout master .

bash scripts/docs-build.sh

git add .

git commit -am "Update book"

echo "git push"
