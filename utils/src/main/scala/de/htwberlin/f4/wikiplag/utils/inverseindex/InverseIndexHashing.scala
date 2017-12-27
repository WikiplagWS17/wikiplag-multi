package de.htwberlin.f4.wikiplag.utils.inverseindex

/**
  * The Hash function used by the inverse index builder.
  */
object InverseIndexHashing  {

  def hash(xs: List[String]): Long = {
   var h:Long = 0

    if (xs.nonEmpty) {
      xs.foreach(x => h=h * 31 + hashCode(x))
    }
    h
  }

  def hashCode(s: String):Long= {
    var value = s.getBytes()
    var h:Long = 0

    if (value.length > 0) {
      value.foreach(x => h=h * 31 + x)
    }
    h
  }
}
