import ot.AppConfig._

/** Starts main process [[ot.dispatcher.SuperVisor]].
 *
 * @author Andrey Starchenkov (astarchenkov@ot.ru)
 */
object SuperDriver extends App {

  import org.apache.log4j.{Logger, Level}

  val log: Logger = Logger.getLogger("DriverLogger")
  log.setLevel(Level.toLevel(getLogLevel(config, "driver")))

  log.info("SuperDriver started.")

  import ot.dispatcher.SuperVisor

  val sv = new SuperVisor()
  sv.run()
}
