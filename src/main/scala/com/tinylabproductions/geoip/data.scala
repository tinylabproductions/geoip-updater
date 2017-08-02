package com.tinylabproductions.geoip

case class CountryCode (code: String)
object CountryCode {
  def validate(code: String): Either[String, CountryCode] = {
    val maxLength = 2
    if (code.length > maxLength) Left(s"Country code '$code' is longer than $maxLength chars!")
    else Right(apply(code.toLowerCase))
  }
}
