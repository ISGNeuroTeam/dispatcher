package ot.scalaotl.commands

import org.apache.spark.sql.functions.col
import ot.scalaotl.Converter

class OTLTransactionTest extends CommandTest {


  override val dataset: String =
    """[
      |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\"}","_nifi_time":"1568037188486","serialField":"0","random_Field":"100","WordField":"qwe","junkField":"q2W","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488751"},
      |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\"}","_nifi_time":"1568037188487","serialField":"1","random_Field":"-90","WordField":"rty","junkField":"132_.","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_nifi_time":"1568037188487","serialField":"2","random_Field":"50","WordField":"uio","junkField":"asd.cx","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037482371"},
      |{"_time":1568026471894,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\"}","_nifi_time":"1568037188487","serialField":"2","random_Field":"50","WordField":"uio","junkField":"asd.cx","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\"}","_nifi_time":"1568037188487","serialField":"3","random_Field":"20","WordField":"GreenPeace","junkField":"XYZ","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_nifi_time":"1568037188487","serialField":"4","random_Field":"30","WordField":"fgh","junkField":"123_ASD","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568024476951,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"4\", \"random_Field\": \"30\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\"}","_nifi_time":"1568037188487","serialField":"4","random_Field":"30","WordField":"fgh","junkField":"123_ASD","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488272"},
      |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_nifi_time":"1568037188487","serialField":"5","random_Field":"50","WordField":"jkl","junkField":"casd(@#)asd","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026472834,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\"}","_nifi_time":"1568037188487","serialField":"5","random_Field":"50","WordField":"jkl","junkField":"casd(@#)asd","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488322"},
      |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\"}","_nifi_time":"1568037188487","serialField":"6","random_Field":"60","WordField":"zxc","junkField":"QQQ.2","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"7\", \"random_Field\": \"-100\", \"WordField\": \"RUS\", \"junkField\": \"00_3\"}","_nifi_time":"1568037188487","serialField":"7","random_Field":"-100","WordField":"RUS","junkField":"00_3","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\"}","_nifi_time":"1568037188487","serialField":"8","random_Field":"0","WordField":"MMM","junkField":"112","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      |{"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\"}","_nifi_time":"1568037188487","serialField":"9","random_Field":"10","WordField":"USA","junkField":"word","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"}
      |]"""

  test("Test 1. Command: Dataset with _time, args includes not all fields") {
    val query = createQuery(
      """table _time, random_Field, WordField
        || transaction random_Field, WordField""", "otstats", s"$test_index")
    var actual = new Converter(query).run
    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    val expected =
      """[
        |{"random_Field":"-100","WordField":"RUS","_time":1568026476854},
        |{"random_Field":"20","WordField":"GreenPeace","_time":1568026476854},
        |{"random_Field":"0","WordField":"MMM","_time":1568026476854},
        |{"random_Field":"100","WordField":"qwe","_time":1568026476854},
        |{"random_Field":"50","WordField":"uio","_time":1568026471894},
        |{"random_Field":"50","WordField":"jkl","_time":1568026472834},
        |{"random_Field":"30","WordField":"fgh","_time":1568024476951},
        |{"random_Field":"10","WordField":"USA","_time":1568026476854},
        |{"random_Field":"-90","WordField":"rty","_time":1568026476854},
        |{"random_Field":"60","WordField":"zxc","_time":1568026476854}
        |]""".stripMargin
    compareDataFrames(actual, jsonToDf(expected))
  }

  test("Test 2. Command: Dataset with _time, args includes all fields") {
    val query = createQuery(
      """table _time, random_Field, WordField
        || transaction _time, random_Field, WordField""", "otstats", s"$test_index")
    var actual = new Converter(query).run
    actual = actual.select(actual.columns.sorted.toSeq.map(c => col(c)):_*)
    val expected =
      """[
        |{"_time":1568026476854,"random_Field":"50","WordField":"jkl"},
        |{"_time":1568026476854,"random_Field":"10","WordField":"USA"},
        |{"_time":1568026476854,"random_Field":"50","WordField":"uio"},
        |{"_time":1568026476854,"random_Field":"100","WordField":"qwe"},
        |{"_time":1568026476854,"random_Field":"20","WordField":"GreenPeace"},
        |{"_time":1568026476854,"random_Field":"30","WordField":"fgh"},
        |{"_time":1568026476854,"random_Field":"-90","WordField":"rty"},
        |{"_time":1568026471894,"random_Field":"50","WordField":"uio"},
        |{"_time":1568026472834,"random_Field":"50","WordField":"jkl"},
        |{"_time":1568026476854,"random_Field":"-100","WordField":"RUS"},
        |{"_time":1568026476854,"random_Field":"0","WordField":"MMM"},
        |{"_time":1568024476951,"random_Field":"30","WordField":"fgh"},
        |{"_time":1568026476854,"random_Field":"60","WordField":"zxc"}
        |]""".stripMargin
    compareDataFrames(actual, jsonToDf(expected))
  }

  test("Test 3. Command: Dataset without _time, args includes not all fields") {
    val query = createQuery(
      """table WordField, random_Field, junkField, serialField
        || transaction random_Field, WordField""", "otstats", s"$test_index")
    val actual = new Converter(query).run
    compareDataFrames(actual, spark.emptyDataFrame)
  }

  test("Test 4. Command: Dataset without _time, args includes all fields") {
    val query = createQuery(
      """table WordField, random_Field, junkField, serialField
        || transaction WordField, random_Field, junkField, serialField""", "otstats", s"$test_index")
    val actual = new Converter(query).run
    compareDataFrames(actual, spark.emptyDataFrame)
  }

  test("Test 5. Command: Dataset contains only _time field (with nulls), args includes only _time") {
    val query = createQuery(
      """table WordField | table _time | transaction _time""", "otstats", s"$test_index")
    val actual = new Converter(query).run
    compareDataFrames(actual, spark.emptyDataFrame)
  }


}