package frameless

import org.apache.spark.sql.types._

import org.scalacheck.Prop._

class IsomorphismTests extends TypedDatasetSuite {
  test("isomorphism") {
    check(forAll { (data: Vector[Usd]) =>
      TypedDataset.create(data).collect().run.toVector ?= data
    })

    check(secure {
      // check that `Usd` is replaced by `DecimalType` in resulting schema
      val field = StructField("a", DecimalType.SYSTEM_DEFAULT, nullable = false, Metadata.empty)

      TypedEncoder[X1[Usd]].targetDataType ?= StructType(field :: Nil)
    })
  }
}
