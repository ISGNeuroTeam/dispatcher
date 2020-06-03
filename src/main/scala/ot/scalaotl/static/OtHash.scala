package ot.scalaotl
package static

import java.security.MessageDigest
import java.math.BigInteger

object OtHash {
  val md5 = (s: String) => new BigInteger(1, MessageDigest.getInstance("MD5").digest(s.getBytes)).toString(16)
  def sha1 = (s: String) => new BigInteger(1, MessageDigest.getInstance("SHA1").digest(s.getBytes)).toString(16)
}