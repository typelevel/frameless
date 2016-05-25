# Frameless

[![Travis Badge](https://travis-ci.org/adelbertc/frameless.svg?branch=master)](https://travis-ci.org/adelbertc/frameless)
[![Codecov Badge](https://codecov.io/gh/adelbertc/frameless/branch/master/graph/badge.svg)](https://codecov.io/gh/adelbertc/frameless)
[![Gitter Badge](https://badges.gitter.im/adelbertc/frameless.svg)](https://gitter.im/adelbertc/frameless?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Frameless is a proof-of-concept library for working with [Spark](http://spark.apache.org/) using more expressive types.
It consists of the following modules:

* `cats` for using Spark with [cats](https://github.com/typelevel/cats)
* `dataframe` for more strongly typed `DataFrame`s
* `dataset` for more strongly typed `Dataset`s

The Frameless project and contributors support the
[Typelevel](http://typelevel.org/) [Code of Conduct](http://typelevel.org/conduct.html) and want all its
associated channels (e.g. GitHub, Gitter) to be a safe and friendly environment for contributing and learning.


## Why?

Benefits of using `TypedDataset` compared to vanilla `Dataset`:

* Typesafe columns referencing and expressions
* Customizable, typecafe encoders
* Enhanced type signature for some built-in functions

## Quick Start
Frameless is compiled against Scala 2.11.x. To use frameless add the following dependencies as needed:

```scala
resolvers += Resolver.sonatypeRepo("snapshots")

val framelessVersion = "0.1.0-SNAPSHOT"

libraryDependencies ++= List(
  "io.github.adelbertc" %% "frameless-cats"      % framelessVersion,
  "io.github.adelbertc" %% "frameless-dataset"   % framelessVersion,
  "io.github.adelbertc" %% "frameless-dataframe" % framelessVersion
)
```

Currently the only release is "0.1.0-SNAPSHOT" which is updated by Travis after every successful build. Frameless
is rapidly changing so be very careful if you depend on this snapshot.

## Development
We require at least *one* sign-off (thumbs-up, +1, or similar) to merge pull requests. The current maintainers
(people who can merge pull requests) are:

* [adelbertc](https://github.com/adelbertc)
* [jeremyrsmith](https://github.com/jeremyrsmith)
* [non](https://github.com/non)
* [OlivierBlanvillain](https://github.com/OlivierBlanvillain/)

## License
Code is provided under the Apache 2.0 license available at http://opensource.org/licenses/Apache-2.0,
as well as in the LICENSE file. This is the same license used as Spark.
