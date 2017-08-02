package com.tinylabproductions.geoip.updater

import java.io.ByteArrayInputStream
import java.nio.file.Path

import akka.actor.{Actor, Cancellable, Props}
import akka.event.Logging
import akka.stream.ActorMaterializer
import com.maxmind.geoip2.DatabaseReader
import com.tinylabproductions.geoip.GeoIP
import com.typesafe.scalalogging.Logger

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object GeoIPUpdaterActor {
  private case class Downloaded(result: Try[Either[String, Array[Byte]]])
  private case object Update

  def props(
    updateDb: DatabaseReader => Unit,
    path: Path,
    uris: GeoIPUpdater.Uris,
    refreshFrequency: FiniteDuration
  )(implicit logger: Logger): Props = {
    Props(new GeoIPUpdaterActor(
      bytes => GeoIP.load(Left(new ByteArrayInputStream(bytes))),
      updateDb,
      bytes => Try(GeoIP.persist(bytes, path)),
      uris, refreshFrequency
    ))
  }
}
class GeoIPUpdaterActor(
  loadDb: Array[Byte] => Try[DatabaseReader],
  updateDb: DatabaseReader => Unit,
  persistDb: Array[Byte] => Try[Unit],
  uris: GeoIPUpdater.Uris,
  refreshFrequency: FiniteDuration
) extends Actor {
  import context.{dispatcher, system}
  private implicit val materializer = ActorMaterializer()(context)
  private[this] val log = Logging(context.system, this)

  log.info(s"Scheduling updates every $refreshFrequency")
  schedule()

  private[this] def schedule(): Cancellable =
    context.system.scheduler.scheduleOnce(
      refreshFrequency, self, GeoIPUpdaterActor.Update
    )

  override def receive = {
    case GeoIPUpdaterActor.Update =>
      log.info("Initiating DB update")
      GeoIPUpdater.update(uris).onComplete { res =>
        self ! GeoIPUpdaterActor.Downloaded(res)
        schedule()
      }
    // Can't use Try#fold because it's non-existent in 2.11
    case GeoIPUpdaterActor.Downloaded(util.Failure(t)) =>
      log.error(t, "Download failed")
    case GeoIPUpdaterActor.Downloaded(util.Success(Left(err))) =>
      log.error(s"Download failed: $err")
    case GeoIPUpdaterActor.Downloaded(util.Success(Right(bytes))) =>
      log.info("Download succeeded.")
      loadDb(bytes) match {
        case util.Failure(t) =>
          log.error(t, s"Can't load downloaded database, not persisting!")
        case util.Success(dr) =>
          updateDb(dr)
          log.info(s"Database updated, persisting.")
          persistDb(bytes) match {
            case util.Failure(err) => log.error(err, s"Persisting failed.")
            case util.Success(_) => log.info(s"Persisted.")
          }
      }
  }
}
