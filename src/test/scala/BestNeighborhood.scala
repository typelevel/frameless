// package typedframe

// import eu.timepit.refined.auto._
// import org.apache.spark.sql.{SpecWithContext, DataFrame}
// import org.scalatest.Matchers._

// object NLPLib {
//   def soundsLikeAGirlName(name: String): Boolean = true
// }

// trait Address

// case class ParisPhoneBookEntry(address: Address, residents: String, phoneNumber: Int)
// case class CityMapEntry(address: Address, neighborhood: String)

// case class Family(residents: String, neighborhood: String)
// case class Person(name: String, neighborhood: String)
// case class NeighborhoodCount(neighborhood: String, count: Long)

// class BestNeighborhood extends SpecWithContext {
//   import testImplicits._
  
//   test("BestNeighborhood") {
//     val phoneBook: TypedFrame[ParisPhoneBookEntry] = TypedFrame(Seq((1, 2)).toDF)
//     val cityMap: TypedFrame[CityMapEntry] = TypedFrame(Seq((1, 2)).toDF)

//     val bestNeighborhood: String =          ((((((((((
//       phoneBook
//         .innerJoin(cityMap).using('address) :TypedFrame[(Address, String, Int, String)])
//         .select('_2, '_4)                   :TypedFrame[(String, String)])
//         .as[Family]()                       :TypedFrame[Family])
//         .flatMap { f =>
//           f.residents.split(' ').map(r => Person(r, f.neighborhood))
//         }                                   :TypedFrame[Person])
//         .filter { p =>
//           NLPLib.soundsLikeAGirlName(p.name)
//         }                                   :TypedFrame[Person])
//         .groupBy('neighborhood).count()     :TypedFrame[(String, String, Long)])
//         .drop('_1)                          :TypedFrame[(String, Long)])
//         .as[NeighborhoodCount]()            :TypedFrame[NeighborhoodCount])
//         .orderBy('count)                    :TypedFrame[NeighborhoodCount])
//         .select('neighborhood)              :TypedFrame[Tuple1[String]])
//         .head
//   }
// }
