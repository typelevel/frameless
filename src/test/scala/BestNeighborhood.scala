package typedframe

import eu.timepit.refined.auto._
import org.apache.spark.sql.{SpecWithContext, DataFrame}
import org.scalatest.Matchers._

object NLPLib {
  def soundsLikeAGirlName(name: String): Boolean = name.toLowerCase.contains("a")
}

object ProblemStatement {
  // Find the neighborhood with most girls
  
  val right = "right"
  val left = "left"
  val top = "top"
  
  type Address = String
  type Neighborhood = String
  
  case class PhoneBookEntry(address: Address, residents: String, phoneNumber: Double)
  case class CityMapEntry(address: Address, neighborhood: Neighborhood)

  case class Family(residents: String, neighborhood: Neighborhood)
  case class Person(name: String, neighborhood: Neighborhood)
  case class NeighborhoodCount(neighborhood: Neighborhood, count: Long)
  
  def phoneBook: Seq[(Address, Neighborhood, Double)] = Seq(
    ("Boulevard de Belleville",      "Inès Enzo Léa",            0.136903816),
    ("Place de la Bourse",           "Louis Jade",               0.170688543),
    ("Avenue de Breteuil",           "Gabriel",                  0.193228634),
    ("Boulevard des Capucines",      "Manon Jules Louise Timéo", 0.113135474),
    ("Avenue des Champs-Élysées",    "Zoé Hugo",                 0.146991315),
    ("Rue de Courcelles",            "Lilou Arthur Léna",        0.175124256),
    ("Rue du Faubourg-Saint-Honoré", "Ethan Sarah",              0.139750951),
    ("Avenue Foch",                  "Maël Maëlys Tom",          0.126858629))
  
  def cityMap: Seq[(Address, Neighborhood)] = Seq(
    ("Boulevard de Belleville", right),
    ("Place de la Bourse", right),
    ("Avenue de Breteuil", left),
    ("Boulevard des Capucines", top),
    ("Avenue des Champs-Élysées", top),
    ("Rue de Courcelles", top),
    ("Rue du Faubourg-Saint-Honoré", top),
    ("Avenue Foch", left))
}

class BestNeighborhood extends SpecWithContext {
  import testImplicits._
  
  implicit class DebugTypedFrame[S <: Product](s: TypedFrame[S]) {
    def d: TypedFrame[S] = { /* s.show(); */ s }
  }
  
  test("complete example") {
    import ProblemStatement._
    
    val phoneBookTF = new TypedFrame[PhoneBookEntry](phoneBook.toDF)
    val cityMapTF = new TypedFrame[CityMapEntry](cityMap.toDF)
    
    val bestNeighborhood: String =            (((((((((
      phoneBookTF
        .innerJoin(cityMapTF).using('address) :TypedFrame[(Address, String, Double, String)]).d
        .select('_2, '_4)                     :TypedFrame[(String, String)]).d
        .as[Family]()                         :TypedFrame[Family]).d
        .flatMap { f =>
          f.residents.split(' ').map(r => Person(r, f.neighborhood))
        }                                     :TypedFrame[Person]).d
        .filter { p =>
          NLPLib.soundsLikeAGirlName(p.name)
        }                                     :TypedFrame[Person]).d
        .groupBy('neighborhood).count()       :TypedFrame[(String, Long)]).d
        .as[NeighborhoodCount]()              :TypedFrame[NeighborhoodCount]).d
        .sortDesc('count)                     :TypedFrame[NeighborhoodCount]).d
        .select('neighborhood)                :TypedFrame[Tuple1[String]]).d
        .head._1
    
    bestNeighborhood shouldBe top
  }
}
