package ot.scalaotl.commands

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{Column, DataFrame, functions => F}
import ot.scalaotl.Converter

class OTLReturnTest extends CommandTest {

  test("Test 1. Command: | Return value from subsearch") {
    val ssQuery = createQuery(s"sort -serialField | return $$serialField",
      "otstats", s"$test_index")
    val dfc = new Converter(ssQuery).run
    val cache = Map("id1" -> dfc)
    val mainQuery = createQuery("""table _time, serialField | eval max_serialField=subsearch=id1""",
      "otstats", s"$test_index")
    var actual = new Converter(mainQuery, cache).run
    actual = setNullableStateOfColumn(actual, "max_serialField", nullable = true)
    val expected = readIndexDF(test_index)
      .select(F.col("_time"), F.col("serialField"))
      .withColumn("max_serialField", F.max("serialField") over ())
      .withColumn("max_serialField", col("max_serialField").cast(IntegerType))
    compareDataFrames(actual, expected)
  }

  test("Test 2. Command: | Return value from subsearch with wrong field name ") {
    val ssQuery = createQuery(s"sort -serialField | return $$serial_Field",
      "otstats", s"$test_index")
    val dfc = new Converter(ssQuery).run
    val cache = Map("id1" -> dfc)
    val mainQuery = createQuery("""table _time, serialField | eval max_serialField=subsearch=id1""",
      "otstats", s"$test_index")
    val thrown = intercept[Exception] {
      new Converter(mainQuery, cache).run
    }
    assert(thrown.getMessage.endsWith("Command eval should have at least one argument"))
  }

  test("Test 3. Command: | return expression for eval") {
    val ssQuery = createQuery(s"""eval val= "value = tonumber(serialField) * tonumber(random_Field)" | return $$val""",
      "otstats", s"$test_index")
    val dfc = new Converter(ssQuery).run
    val cache = Map("id1" -> dfc)
    val mainQuery = createQuery("""table _time, serialField, random_Field | eval subsearch=id1""",
      "otstats", s"$test_index")
    var actual = new Converter(mainQuery, cache).run
    actual = setNullableStateOfColumn(actual, "value", nullable = true)
    val expected = readIndexDF(test_index)
      .select(F.col("_time"), F.col("serialField"), F.col("random_Field"))
      .withColumn("value", F.col("serialField").cast(DoubleType) * F.col("random_Field").cast(DoubleType))
    compareDataFrames(actual, expected)
  }


  test("Test 4. Command: | Return multiple values from subsearch") {
    val mainQuery = createQuery(s"sort -serialField | return 5 serialField",
      "otstats", s"$test_index")
    var actual = new Converter(mainQuery).run
    actual = setNullableStateOfColumn(actual, "serialField", nullable = true)
    val expected = readIndexDF(test_index)
      .orderBy(F.col("serialField").desc)
      .select(F.col("serialField")).limit(5)
    compareDataFrames(actual, expected)
  }
}
