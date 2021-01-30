# Injection: Creating Custom Encoders

```scala mdoc:invisible
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import frameless.functions.aggregate._
import frameless.TypedDataset

val conf = new SparkConf().setMaster("local[*]").setAppName("frameless repl").set("spark.ui.enabled", "false")
implicit val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

import spark.implicits._
```
Injection lets us define encoders for types that do not have one by injecting `A` into an encodable type `B`.
This is the definition of the injection typeclass:
```scala
trait Injection[A, B] extends Serializable {
  def apply(a: A): B
  def invert(b: B): A
}
```

## Example

Let's define a simple case class:

```scala mdoc
case class Person(age: Int, birthday: java.util.Date)
val people = Seq(Person(42, new java.util.Date))
```

And an instance of a `TypedDataset`:

```scala mdoc:fail
val personDS = TypedDataset.create(people)
```

Looks like we can't, a `TypedEncoder` instance of `Person` is not available, or more precisely for `java.util.Date`.
But we can define a injection from `java.util.Date` to an encodable type, like `Long`:

```scala mdoc
import frameless._
implicit val dateToLongInjection = new Injection[java.util.Date, Long] {
  def apply(d: java.util.Date): Long = d.getTime()
  def invert(l: Long): java.util.Date = new java.util.Date(l)
}
```

We can be less verbose using the `Injection.apply` function:

```scala mdoc
import frameless._
implicit val dateToLongInjection = Injection((_: java.util.Date).getTime(), new java.util.Date((_: Long)))
```

Now we can create our `TypedDataset`:

```scala mdoc
val personDS = TypedDataset.create(people)
```

## Another example

Let's define a sealed family:

```scala mdoc
sealed trait Gender
case object Male extends Gender
case object Female extends Gender
case object Other extends Gender
```

And a simple case class:

```scala mdoc
case class Person(age: Int, gender: Gender)
val people = Seq(Person(42, Male))
```

Again if we try to create a `TypedDataset`, we get a compilation error.

```scala mdoc:fail
val personDS = TypedDataset.create(people)
```

Let's define an injection instance for `Gender`:

```scala mdoc
implicit val genderToInt: Injection[Gender, Int] = Injection(
  {
    case Male   => 1
    case Female => 2
    case Other  => 3
  },
  {
    case 1 => Male
    case 2 => Female
    case 3 => Other
  })
```

And now we can create our `TypedDataset`:

```scala mdoc
val personDS = TypedDataset.create(people)
```

```scala mdoc:invisible
spark.stop()
```
