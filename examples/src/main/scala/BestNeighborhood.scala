package typedframe

object NLPLib {
  def soundsLikeAGirlName(name: String): Boolean
}

case class ParisPhoneBookEntry(address: Address, residents: String, phoneNumber: Int)
case class CityMapEntry(address: Address, neighborhood: String)

val phoneBook = TypedFrame[ParisPhoneBookEntry](...)
val cityMap = TypedFrame[CityMapEntry](...)

case class Family(residents: String, neighborhood: String)
case class Person(name: String, neighborhood: String)
case class NeighborhoodCount(neighborhood: String, count: Long)

val bestNeighborhood: String =      ((((((((((
  phoneBook
    .join(cityMap).using('address)  :TypedFrame[(Address, String, Int, String)])
    .select('_2, '_4)               :TypedFrame[(String, String)])
    .as[Family]()                   :TypedFrame[Family])
    .flatMap { f =>
      f.residents.split(' ').map(r => Person(r, f.neighborhood)
    }                               :TypedFrame[Person])
    .filter { p =>
      NLPLib.soundsLikeAGirlName(p.name)
    }                               :TypedFrame[Person])
    .grouBy('neighborhood).count()  :TypedFrame[(String, String, Long)])
    .drop('_1)                      :TypedFrame[(String, Long)])
    .as[NeighborhoodCount]()        :TypedFrame[NeighborhoodCount])
    .orderBy('count)                :TypedFrame[NeighborhoodCount])
    .select('neighborhood)          :TypedFrame[Tuple1[String]])
    .head
