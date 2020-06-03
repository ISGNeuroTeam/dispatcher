package ot.scalaotl.commands

import org.scalatest.Ignore

@Ignore
class OTLReturnTest extends CommandTest {

//  test("Test 0. Command: | return serialField ") {
//    val actual = execute(""" table serialField | return 5 serialField """)
//    val expected = """[
//                     |{"search":"(serialField=\"0\") OR (serialField=\"1\") OR (serialField=\"2\") OR (serialField=\"3\") OR (serialField=\"4\")"}
//                     |]""".stripMargin
//    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
//  }
//
//  test("Test 1. Command: | return 1 serialField WordField ") {
//    val actual = execute(""" table serialField WordField | return serialField WordField""")
//    val expected = """[
//                     |{"search":"serialField=\"0\" WordField=\"qwe\""}
//                     |]""".stripMargin
//    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
//  }
//
//  test("Test 2. Command: | return serialField WordField") {
//    val actual = execute(""" table serialField WordField | return 5 serialField WordField""")
//    val expected = """[
//                     |{"search":"(serialField=\"0\" WordField=\"qwe\") OR (serialField=\"1\" WordField=\"rty\") OR (serialField=\"2\" WordField=\"uio\") OR (serialField=\"3\" WordField=\"GreenPeace\") OR (serialField=\"4\" WordField=\"fgh\")"}
//                     |]""".stripMargin
//    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
//  }
//  test("Test 3. Command: | return $serialField WordField") {
//    val actual = execute("""  return 5 $serialField WordField""")
//    val expected = """[
//                     |{"search":"(0 WordField=\"qwe\") OR (1 WordField=\"rty\") OR (2 WordField=\"uio\") OR (3 WordField=\"GreenPeace\") OR (4 WordField=\"fgh\")"}
//                     |]""".stripMargin
//    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
//  }
}
