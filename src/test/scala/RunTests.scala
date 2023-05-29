import org.scalatest.tools.Runner

object RunTests {

  def main(args: Array[String]): Unit = {
    Runner.run(Array("-o", "-R", "/home/rkpvteh/src/dispatcher/src/test/scala/ot/scalaotl/commands/"))
  }
}
