#!/bin/bash

set -eux

# Check that the working directory is a git repository and the repository has no outstanding changes.
git diff-index --quiet HEAD

commit=$(git show -s --format=%h)

git checkout gh-pages

git merge "$commit"

bash scripts/docs-build.sh

git add .

git commit -am "Rebuild documentation ($commit)"

echo "Verify that you didn't break anything:"
echo "  $ python -m SimpleHTTPServer 8000"
echo "  $ xdg-open http://localhost:8000/"
echo ""
echo "Then push to the gh-pages branch:"
echo "  $ git push gh-pages"
