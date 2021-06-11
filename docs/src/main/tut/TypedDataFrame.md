# Proof of Concept: TypedDataFrame

`TypedDataFrame` is the API developed in the early stages of Frameless to manipulate Spark `DataFrame`s in a type-safe manner. With the introduction of `Dataset` in Spark 1.6, `DataFrame` seems deprecated and won't be the focus of future development of Frameless. However, the design is interesting enough to document.

To safely manipulate `DataFrame`s we use a technique called a *shadow type*, which consists in storing additional information about a value in a "dummy" type. Mirroring value-level computation at the type-level lets us leverage the type system to catch common mistakes at compile time.

### Diving in

In `TypedDataFrame`, we use a single `Schema <: Product` to model the number, the types and the names of columns. Here is a what the definition of `TypedDataFrame` looks like, with simplified type signatures:

```scala
import org.apache.spark.sql.DataFrame
import shapeless.HList

class TDataFrame[Schema <: Product](df: DataFrame) {
  def filter(predicate: Schema => Boolean): TDataFrame[Schema] = ???

  def select[C <: HList, Out <: Product](columns: C): TDataFrame[Out] = ???

  def innerJoin[OtherS <: Product, Out <: Product]
    (other: TDataFrame[OtherS]): TDataFrame[Out] = ???

  // Followed by equivalent of every DataFrame method with improved signature
}
```

As you can see, instead of the `def filter(conditionExpr: String): DataFrame` defined in Spark, the `TypedDataFrame` version expects a function from `Schema` to `Boolean`, and models the fact that resulting `DataFrame` will still hold elements of type `Schema`.

### Type-level column referencing

For Spark's `DataFrame`s, column referencing is done directly by `String`s or using the `Column` type which provides no additional type safety. `TypedDataFrame` improves on that by catching invalid column references compile type. When everything goes well, Frameless select is very similar to vanilla select, except that it keeps track of the selected column types:

```scala
import frameless.TypedDataFrame

case class Foo(s: String, d: Double, i: Int)

def selectIntString(tf: TypedDataFrame[Foo]): TypedDataFrame[(Int, String)] =
  tf.select('i, 's)
```

However, in case of typo, it gets caught right away:

```scala
def selectIntStringTypo(tf: TypedDataFrame[Foo]): TypedDataFrame[(Int, String)] =
  tf.select('j, 's)
```

### Type-level joins

Joins can available with two different syntaxes. The first lets you reference different columns on each `TypedDataFrame`, and ensures that they all exist and have compatible types:

```scala
case class Bar(i: Int, j: String, b: Boolean)

def join1(tf1: TypedDataFrame[Foo], tf2: TypedDataFrame[Bar])
    : TypedDataFrame[(String, Double, Int, Int, String, Boolean)] =
  tf1.innerJoin(tf2).on('s).and('j)
```

The second syntax brings some convenience when the joining columns have identical names in both tables:

```scala
def join2(tf1: TypedDataFrame[Foo], tf2: TypedDataFrame[Bar])
    : TypedDataFrame[(String, Double, Int, String, Boolean)] =
  tf1.innerJoin(tf2).using('i)
```

Further example are available in the [TypedDataFrame join tests.](https://github.com/typelevel/frameless/blob/17194d2172e75f8994e9481181e85b4c8dcc0f69/dataframe/src/test/scala/JoinTests.scala)

### Complete example

We now consider a complete example to see how the Frameless types can improve not only correctness but also the readability of Spark jobs. Consider the following domain of phonebooks, city maps and neighborhoods:

```scala mdoc:silent
type Neighborhood = String
type Address = String

case class PhoneBookEntry(
  address: Address,
  residents: String,
  phoneNumber: Double
)

case class CityMapEntry(
  address: Address,
  neighborhood: Neighborhood
)
```

Our goal will be to compute the neighborhood with unique names, approximating "unique" with names containing less common
letters in the alphabet: 'x', 'q', and 'z'. We are going to need a natural language processing library at some point, so
let's use the following for the example:

```scala mdoc:silent
object NLPLib {
  def uniqueName(name: String): Boolean = name.exists(Set('x', 'q', 'z'))
}
```

Suppose we manage to obtain public data for a `TypedDataFrame[PhoneBookEntry]` and `TypedDataFrame[CityMapEntry]`. Here is what our Spark job could look like with Frameless:

```scala
import org.apache.spark.sql.SQLContext

// These case classes are used to hold intermediate results
case class Family(residents: String, neighborhood: Neighborhood)
case class Person(name: String, neighborhood: Neighborhood)
case class NeighborhoodCount(neighborhood: Neighborhood, count: Long)

def bestNeighborhood
  (phoneBookTF: TypedDataFrame[PhoneBookEntry], cityMapTF: TypedDataFrame[CityMapEntry])
  (implicit c: SQLContext): String = {
                                          (((((((((
  phoneBookTF
    .innerJoin(cityMapTF).using('address) :TypedDataFrame[(Address, String, Double, String)])
    .select('_2, '_4)                     :TypedDataFrame[(String, String)])
    .as[Family]()                         :TypedDataFrame[Family])
    .flatMap { f =>
      f.residents.split(' ').map(r => Person(r, f.neighborhood))
    }                                     :TypedDataFrame[Person])
    .filter { p =>
      NLPLib.uniqueName(p.name)
    }                                     :TypedDataFrame[Person])
    .groupBy('neighborhood).count()       :TypedDataFrame[(String, Long)])
    .as[NeighborhoodCount]()              :TypedDataFrame[NeighborhoodCount])
    .sortDesc('count)                     :TypedDataFrame[NeighborhoodCount])
    .select('neighborhood)                :TypedDataFrame[Tuple1[String]])
    .head._1
}
```

If you compare this version to vanilla Spark where every line is a `DataFrame`, you see how much types can improve readability. An executable version of this example is available in the [BestNeighborhood test](https://github.com/typelevel/frameless/blob/17194d2172e75f8994e9481181e85b4c8dcc0f69/dataframe/src/test/scala/BestNeighborhood.scala).

### Limitations

The main limitation of this approach comes from Scala 2.10, which limits the arity of class classes to 22. Because of the way `DataFrame` models joins, joining two table with more that 11 fields results in a `DataFrame` which not representable with `Schema` of type `Product`.

In the `Dataset` API introduced in Spark 1.6, the way join are handled was rethought to return a pair of both schemas instead of a flat table, which moderates the trouble caused by case class limitations. Alternatively, since Scala 2.11, it is possible to define Tuple23 and onward. Sadly, due to the way Spark is commonly packaged in various systems, the amount Spark users having to Scala 2.11 and *not* to Spark 1.6 is essentially zero. For this reasons, further development in Frameless will target Spark 1.6+, deprecating the early work on`TypedDataFrame`.
