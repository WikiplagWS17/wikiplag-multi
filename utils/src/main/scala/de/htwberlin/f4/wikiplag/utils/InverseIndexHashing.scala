package de.htwberlin.f4.wikiplag.utils

import scala.util.hashing.Hashing

/**
  * The Hash function used by the inverse index builder.
  */
object InverseIndexHashing extends Hashing[List[String]] {

  override def hash(xs: List[String]): Int = {
   //we just use the default implementation but it is to be changed some day it will be easy to do so
    xs.hashCode()
  }
}
