#!/bin/bash

# Taken + modified from typelevel/cats
# https://github.com/typelevel/cats/blob/a8a7587f558541cbabc5c40053181928b4baf78c/scripts/travis-publish.sh

export publish_cmd="publishLocal"

# if [[ $TRAVIS_PULL_REQUEST == "false" && $TRAVIS_BRANCH == "master" && $(cat version.sbt) =~ "-SNAPSHOT" ]]; then
#   export publish_cmd="common/publish cats/publish dataset/publish dataframe/publish"
# fi

sbt_cmd="sbt ++$TRAVIS_SCALA_VERSION -Dfile.encoding=UTF8 -J-XX:ReservedCodeCacheSize=256M"

case "$PHASE" in
  A) 
     docs_cmd="$sbt_cmd docs/mdoc"
     run_cmd="$docs_cmd"
  ;;
  B)
     coverage="$sbt_cmd coverage test && sbt coverageReport && bash <(curl -s https://codecov.io/bash)"
     run_cmd="$coverage"
  ;;
  C) 
     run_cmd="$sbt_cmd clean $publish_cmd"
  ;;   
esac 
eval $run_cmd
