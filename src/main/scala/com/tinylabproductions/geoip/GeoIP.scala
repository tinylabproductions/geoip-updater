package com.tinylabproductions.geoip

import java.io.{File, InputStream}
import java.net.InetAddress
import java.nio.file.{Files, Path}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.maxmind.db.CHMCache
import com.maxmind.db.Reader.FileMode
import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.exception.AddressNotFoundException
import com.tinylabproductions.geoip.updater.GeoIPUpdater
import com.typesafe.scalalogging.Logger

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

/**
  * Created by arturas on 2016-12-08.
  */
class GeoIP(var db: DatabaseReader) {
  def lookupCountry(clientIp: InetAddress): Option[CountryCode] = {
    try {
      // Love Java and nulls
      for {
        r <- Option(db.country(clientIp))
        r <- Option(r.getCountry)
        r <- Option(r.getIsoCode)
        r <- CountryCode.validate(r.toLowerCase).right.toOption
      } yield r
    }
    catch { case _: AddressNotFoundException => None }
  }
}

object GeoIP {
  def load(source: Either[InputStream, File]): Try[DatabaseReader] = Try {
    source.fold(
      new DatabaseReader.Builder(_),
      // We need to use memory mode for file, because we overwrite the file
      // while it is being used.
      //
      // Also the file is small and fits into memory
      new DatabaseReader.Builder(_).fileMode(FileMode.MEMORY)
    ).withCache(new CHMCache()).build()
  }

  def loadOrDownload(
    path: Path, uris: GeoIPUpdater.Uris
  )(
    implicit log: Logger, actorSystem: ActorSystem, actorMaterializer: ActorMaterializer
  ): Future[GeoIP] = {
    import actorSystem.dispatcher

    def handleLoadDb(
      `try`: Try[DatabaseReader], firstTime: Boolean = true
    ): Future[DatabaseReader] = `try` match {
      case util.Failure(t) =>
        if (firstTime) {
          log.warn(s"Can't load geoip DB at $path, downloading new one.", t)
          GeoIPUpdater.update(uris).flatMap {
            case Left(err) =>
              throw new Exception(s"Can't download GEOIP db! $err")
            case Right(bytes) =>
              persist(bytes, path)
              handleLoadDb(load(Right(path.toFile)), firstTime = false)
          }
        }
        else {
          throw new Exception(s"Can't load DB at $path", t)
        }
      case util.Success(db) =>
        log.info(s"GeoIP db loaded from $path")
        Future.successful(db)
    }

    log.info(s"Trying to load geoip db from $path")
    handleLoadDb(load(Right(path.toFile))).map(new GeoIP(_))
  }

  def loadOrDownloadSync(path: Path, uris: GeoIPUpdater.Uris)(implicit logger: Logger): GeoIP = {
    implicit val system = ActorSystem("geoip")
    try {
      implicit val materializer = ActorMaterializer()
      val future = GeoIP.loadOrDownload(path, uris)
      Await.result(future, Duration.Inf)
    }
    finally {
      system.terminate().onComplete { result =>
        logger.debug(s"Actor system ${system.name} terminated: $result")
      }(concurrent.ExecutionContext.Implicits.global)
    }
  }

  def persist(bytes: Array[Byte], path: Path)(implicit log: Logger): Unit = {
    log.info(s"Writing downloaded database to $path")
    Files.write(path, bytes)
    log.info(s"Writing downloaded database to $path")
  }
}
