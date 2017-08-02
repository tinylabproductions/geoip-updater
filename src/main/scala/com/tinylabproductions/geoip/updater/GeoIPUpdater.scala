package com.tinylabproductions.geoip.updater

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.zip.GZIPInputStream

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes, Uri}
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream
import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.IOUtils

import scala.concurrent.Future

/**
  * Created by arturas on 2016-12-07.
  */
object GeoIPUpdater {
  case class Uris(file: Uri, md5: Uri)
  object Uris {
    val default = Uris(
      file = Uri("http://geolite.maxmind.com/download/geoip/database/GeoLite2-Country.mmdb.gz"),
      md5 = Uri("http://geolite.maxmind.com/download/geoip/database/GeoLite2-Country.md5")
    )
  }

  def update(uris: Uris)(
    implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer
  ): Future[Either[String, Array[Byte]]] = {
    import actorSystem.dispatcher

    val md5F = requestString(uris.md5)
    val fileF = request(uris.file)
    (md5F zip fileF).map { case (md5E, fileE) =>
      // Can't use for comprehension because of cross compiling to 2.11
      md5E.right.flatMap { md5Untrimmed =>
        val md5 = md5Untrimmed.trim
        fileE.right.flatMap { file =>
          val stream = new GZIPInputStream(new ByteBufferBackedInputStream(file.asByteBuffer))
          val bytes = IOUtils.toByteArray(stream)
          // digests are mutable and not thread safe
          val md5Digest = MessageDigest.getInstance("MD5")
          val calculatedMd5 = Hex.encodeHexString(md5Digest.digest(bytes))

          // Can't use Either.cond because cross-compiling for 2.11
          if (calculatedMd5 == md5) Right(bytes)
          else Left(
            s"MD5 does not match for downloaded file! Calculated: $calculatedMd5, fetched: $md5"
          )
        }
      }
    }
  }

  def request(uri: Uri)(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer)
  : Future[Either[String, ByteString]] = {
    import actorSystem.dispatcher

    Http().singleRequest(HttpRequest(uri = uri)).flatMap {
      case HttpResponse(StatusCodes.OK, headers @ _, entity, _) =>
        val bytesF = entity.dataBytes.runFold(ByteString(""))(_ ++ _)
        bytesF.map(Right.apply)
      case r @ HttpResponse(code @ _, _, _, _) =>
        Future.successful(Left(s"Non 200 response: $r"))
    }.recover { case t => Left(s"Exception: $t") }
  }

  def requestString(uri: Uri)(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer)
  : Future[Either[String, String]] = {
    import actorSystem.dispatcher

    request(uri).map(_.right.map(_.decodeString(StandardCharsets.UTF_8)))
  }
}
