package frameless

import org.apache.spark.sql.{functions => sfunc}
import frameless.functions.aggregate._

object SortTests {
  case class Wack(w: Int)
  case class Foo(a: String, b: Option[Int], c: Array[String], d: Map[String, Int], wack: Wack)
}
class SortTests extends TypedDatasetSuite {
  import SortTests._

  test("asdf") {
    val ds = TypedDataset.create(Seq(
      Foo("a", Some(2), Array("a", "b"), Map("world" -> 2), Wack(1)),
      Foo("b", Some(1), Array("b", "a"), Map("world" -> 2), Wack(2))
    ))

//    println(ds.collect().run())
//    ds.show().run()

    /*
    This fails at run time and shouldn't be allowed!
    ds.sort(ds('b).asc.isNone.asc).show().run()

 And even this blows up as well.
    ds
      .toDF()
      .sort(sfunc.col("b").asc.isNull)
      .show()

Once you call order, there's no going back
      */

//    ds.sort(ds('b).isNone.asc).show().run()

    /* This won't even compile instead of blowing at runtime!
    ds.sort(ds('d).desc).show().run()
    */

    /* TODO: order by struct
    ds.sort(ds('wack).desc).show().run()
    */

//    ds.selectMany(count(ds('b)))

    ds.sort(ds('a).desc).show().run
    ds.sort(ds('a).desc, ds('b).asc).show().run()

    ds.toDF()
      .sort(sfunc.col("a").desc, sfunc.col("b").asc)
      .show()

    val temp = ds.groupBy(ds('a))
      .mapGroups{ case (l, i) =>
        i.maxBy(_.c.length)
      }

        .filter(ds('b).isNotNone)
      .explain()




//    ds
//      .toDF()
////        .select(sfunc.col("b").desc)
//      .sort(sfunc.col("wack").desc)
//      .show()

  }

}
