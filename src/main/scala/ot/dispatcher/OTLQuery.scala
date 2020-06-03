package ot.dispatcher

/**
 * Returns instance with all needed information about Job from DB.
 *
 * @param id           Job ID.
 * @param original_otl OTL query from request without any changes as is.
 * @param service_otl  OTL query modified for calculation on platform.
 * @param tws          Time Window Start.
 * @param twf          Time Window Finish.
 * @param cache_ttl    Cache time to life.
 * @param indexes      Available indexes for user.
 * @param subsearches  Subsearch queries if present.
 * @author Andrey Starchenkov (astarchenkov@ot.ru)
 */
case class OTLQuery(id: Int, original_otl: String, service_otl: String, tws: Int, twf: Int, cache_ttl: Int,
                    indexes: Array[String], subsearches: Map[String, String], username: String,
                    field_extraction: Boolean, preview: Boolean)

object OTLQuery {
  def apply(query: String) = new OTLQuery(
    id = -1,
    original_otl = query,
    service_otl = query,
    tws = -1,
    twf = -1,
    cache_ttl = -1,
    indexes = Array(),
    subsearches = Map(),
    username = query,
    field_extraction = false,
    preview = false
  )
}
