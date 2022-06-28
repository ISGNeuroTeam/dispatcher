package ot.scalaotl.commands

import org.apache.spark.sql.functions.col
import ot.scalaotl.Converter

class OTLTransactionTest extends CommandTest {

  // вообще не работает
  test("Test 1. Command: Dataset with _time, run transaction random_Field, WordField") {
    val query = createQuery("""table _time, random_Field, WordField | transaction random_Field, WordField""",
      "otstats", s"$test_index")
    var actual = new Converter(query).run
    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    val expected =
      """[
        |{"random_Field":"-100","WordField":"RUS","_time":1568026476854},
        |{"random_Field":"20","WordField":"GreenPeace","_time":1568026476854},
        |{"random_Field":"0","WordField":"MMM","_time":1568026476854},
        |{"random_Field":"100","WordField":"qwe","_time":1568026476854},
        |{"random_Field":"50","WordField":"uio","_time":1568026476854},
        |{"random_Field":"50","WordField":"jkl","_time":1568026476854},
        |{"random_Field":"30","WordField":"fgh","_time":1568026476854},
        |{"random_Field":"10","WordField":"USA","_time":1568026476854},
        |{"random_Field":"-90","WordField":"rty","_time":1568026476854},
        |{"random_Field":"60","WordField":"zxc","_time":1568026476854}
        |]""".stripMargin
    compareDataFrames(actual, jsonToDf(expected))
  }

  test("Test 2. Command: Dataset without _time, run transaction random_Field, WordField") {
    val query = createQuery("""table random_Field, WordField | transaction random_Field, WordField""",
      "otstats", s"$test_index")
    val actual = new Converter(query).run
    val expected =
      """[
        |]""".stripMargin
    compareDataFrames(actual, jsonToDf(expected))
  }
}