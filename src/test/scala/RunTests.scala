import org.scalatest.tools.Runner

object RunTests {

  def main(args: Array[String]): Unit = {
    Runner.run(Array("-o", "-R", "/mnt/glfs/tests/test-classes"))
  }
}
