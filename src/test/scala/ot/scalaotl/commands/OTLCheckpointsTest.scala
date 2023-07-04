package ot.scalaotl.commands

import ot.AppConfig

class OTLCheckpointsTest extends CommandTest {
  test("Test 0. Checkpoints off when checkpoints are on") {
    val actual = execute("checkpoints off")
    assert(!AppConfig.withCheckpoints)
  }

  test("Test 1. Checkpoints on when checkpoints are off") {
    AppConfig.withCheckpoints = false
    val actual = execute("checkpoints on")
    assert(AppConfig.withCheckpoints)
  }

  test("Test 2. Checkpoints off when checkpoints are off") {
    AppConfig.withCheckpoints = false
    val actual = execute("checkpoints off")
    assert(!AppConfig.withCheckpoints)
  }

  test("Test 3. Checkpoints on when checkpoints are on") {
    val actual = execute("checkpoints on")
    assert(AppConfig.withCheckpoints)
  }
}
