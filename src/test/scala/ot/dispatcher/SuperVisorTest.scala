package ot.dispatcher

import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SuperVisorTest extends FunSuite with BeforeAndAfterAll{
  test("") {
    val visor = new SuperVisor
    visor.run()
  }
}
