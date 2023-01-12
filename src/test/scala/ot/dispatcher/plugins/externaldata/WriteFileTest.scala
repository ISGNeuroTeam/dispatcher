package ot.dispatcher.plugins.externaldata

import java.io.File

import ot.dispatcher.plugins.externaldata.commands.WriteFile
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class WriteFileTest extends CommandTest {
  override val dataset: String = """[
      |{"a":"1","b":"2"},
      |{"a":"10","b":"20"}
      |]""".stripMargin

  test("Test 0. Command: | writeFile parquet") {
    val path = new File("src/test/resources/temp/write_test_file_parquet").getAbsolutePath
    val simpleQuery = SimpleQuery(""" format=parquet path=write_test_file_parquet """)
    val commandWriteFile = new WriteFile(simpleQuery, utils)
    execute(commandWriteFile)
    val actual = spark.read.format("parquet").load(path).toJSON.collect().mkString("[\n",",\n","\n]")
    val expected = dataset
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Command: | writeFile json") {
    val path = new File("src/test/resources/temp/write_test_file_json").getAbsolutePath
    val simpleQuery = SimpleQuery(""" format=json path=write_test_file_json """)
    val commandWriteFile = new WriteFile(simpleQuery, utils)
    execute(commandWriteFile)
    val actual = spark.read.format("json").load(path).toJSON.collect().mkString("[\n",",\n","\n]")
    val expected = dataset
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | writeFile csv") {
    val path = new File("src/test/resources/temp/write_test_file_csv").getAbsolutePath
    val simpleQuery = SimpleQuery(""" format=csv path=write_test_file_csv """)
    val commandWriteFile = new WriteFile(simpleQuery, utils)
    execute(commandWriteFile)
    val actual = spark.read.format("csv").option("header", "true").load(path).toJSON.collect().mkString("[\n",",\n","\n]")
    val expected = dataset
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3. Command: | writeFile with default params") {
    val path = new File("src/test/resources/temp/write_test_file_parquet").getAbsolutePath
    val simpleQuery = SimpleQuery(""" path=write_test_file_parquet """)
    val commandWriteFile = new WriteFile(simpleQuery, utils)
    execute(commandWriteFile)
    val actual = spark.read.format("parquet").option("header", "true").load(path).toJSON.collect().mkString("[\n",",\n","\n]")
    val expected = dataset
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 4. Command: | writeFile mode=append") {
    val path = new File("src/test/resources/temp/write_test_file_csv").getAbsolutePath
    val simpleQuery = SimpleQuery(""" format=csv path=write_test_file_csv mode=append """)
    val commandWriteFile = new WriteFile(simpleQuery, utils)
    execute(commandWriteFile)
    val actual = spark.read.format("csv").option("header", "true").load(path).toJSON.collect().mkString("[\n",",\n","\n]")
    val expected = """[
    {"a":"1","b":"2"},
    {"a":"10","b":"20"},
    {"a":"1","b":"2"},
    {"a":"10","b":"20"}
    ]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 5. Command: | writeFile numPartitions=1") {
    val path = new File("src/test/resources/temp/write_test_file_csv").getAbsolutePath
    val simpleQuery = SimpleQuery(""" format=csv path=write_test_file_csv numPartitions=1 """)
    val commandWriteFile = new WriteFile(simpleQuery, utils)
    execute(commandWriteFile)
    val actual = spark.read.format("csv").option("header", "true").load(path).toJSON.collect().mkString("[\n",",\n","\n]")
    val expected = dataset
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 6. Command: | writeFile partitionBy=a") {
    val path = new File("src/test/resources/temp/write_test_file_csv").getAbsolutePath
    val simpleQuery = SimpleQuery(""" format=csv path=write_test_file_csv partitionBy=a """)
    val commandWriteFile = new WriteFile(simpleQuery, utils)
    execute(commandWriteFile)
    val actual = spark.read.format("csv").option("header", "true").load(path).toJSON.collect().mkString("[\n",",\n","\n]")
    val expected = """[
                     |{"b":"20","a":10},
                     |{"b":"2","a":1}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}

