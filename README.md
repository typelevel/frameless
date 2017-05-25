# Frameless

[![Travis Badge](https://travis-ci.org/typelevel/frameless.svg?branch=master)](https://travis-ci.org/typelevel/frameless)
[![Codecov Badge](https://codecov.io/gh/typelevel/frameless/branch/master/graph/badge.svg)](https://codecov.io/gh/typelevel/frameless)
[![Maven Badge](https://img.shields.io/maven-central/v/org.typelevel/frameless-dataset_2.11.svg)](https://maven-badges.herokuapp.com/maven-central/org.typelevel/frameless-dataset_2.11)
[![Gitter Badge](https://badges.gitter.im/typelevel/frameless.svg)](https://gitter.im/typelevel/frameless)

Frameless is a proof-of-concept library for working with [Spark](http://spark.apache.org/) using more expressive types.
It consists of the following modules:

* `dataset` for more strongly typed `Dataset`s  (supports Spark 2.0.x)
* `cats` for using Spark with [cats](https://github.com/typelevel/cats) (supports Cats 0.9.x)


The Frameless project and contributors support the
[Typelevel](http://typelevel.org/) [Code of Conduct](http://typelevel.org/conduct.html) and want all its
associated channels (e.g. GitHub, Gitter) to be a safe and friendly environment for contributing and learning.

## Documentation

* [TypedDataset: Feature Overview](http://typelevel.org/frameless/FeatureOverview.html)
* [Comparing TypedDatasets with Spark's Datasets](http://typelevel.org/frameless/TypedDatasetVsSparkDataset.html)
* [Typed Encoders in Frameless](http://typelevel.org/frameless/TypedEncoder.html)
* [Injection: Creating Custom Encoders](http://typelevel.org/frameless/Injection.html)
* [Job\[A\]](http://typelevel.org/frameless/Job.html)
* [Using Cats with RDDs](http://typelevel.org/frameless/Cats.html)
* [Proof of Concept: TypedDataFrame](http://typelevel.org/frameless/TypedDataFrame.html)

## Why?

Benefits of using `TypedDataset` compared to the standard Spark `Dataset` API:

* Typesafe columns referencing and expressions
* Customizable, typesafe encoders
* Typesafe casting and projections
* Enhanced type signature for some built-in functions

## Quick Start
Frameless is compiled against Scala 2.11.x.

Note that while Frameless is still getting off the ground, it is very possible that breaking changes will be
made for at least the next few versions.

To use Frameless in your project add the following in your `build.sbt` file as needed:

```scala
resolvers += Resolver.sonatypeRepo("releases")

val framelessVersion = "0.3.0"

libraryDependencies ++= List(
  "org.typelevel" %% "frameless-cats"      % framelessVersion,
  "org.typelevel" %% "frameless-dataset"   % framelessVersion
)
```

An easy way to bootstrap a Frameless sbt project:

- if you have [Giter8][g8] installed then simply:

```bash
g8 imarios/frameless.g8
```
- with sbt >= 0.13.13:

```bash
sbt new imarios/frameless.g8
```
Typing `sbt console` inside your project will bring up a shell with Frameless
and all its dependencies loaded (including Spark).

## Development
We require at least *one* sign-off (thumbs-up, +1, or similar) to merge pull requests. The current maintainers
(people who can merge pull requests) are:

* [adelbertc](https://github.com/adelbertc)
* [imarios](https://github.com/imarios)
* [jeremyrsmith](https://github.com/jeremyrsmith)
* [kanterov](https://github.com/kanterov)
* [non](https://github.com/non)
* [OlivierBlanvillain](https://github.com/OlivierBlanvillain/)

## License
Code is provided under the Apache 2.0 license available at http://opensource.org/licenses/Apache-2.0,
as well as in the LICENSE file. This is the same license used as Spark.

[g8]: http://www.foundweekends.org/giter8/
