package ot.scalaotl.commands

import ot.AppConfig
import ot.dispatcher.sdk.core.CustomException

class OTLCheckpointsTest extends CommandTest {
  test("Test 0. Checkpoints off when checkpoints are on") {
    AppConfig.withCheckpoints = true
    val actual = execute("checkpoints off")
    assert(!AppConfig.withCheckpoints)
  }

  test("Test 1. Checkpoints on when checkpoints are off") {
    AppConfig.withCheckpoints = false
    val actual = execute("eval a = 1 | checkpoints on")
    assert(AppConfig.withCheckpoints)
  }

  test("Test 2. Checkpoints off when checkpoints are off") {
    AppConfig.withCheckpoints = false
    val actual = execute("eval a = 1 | checkpoints off")
    assert(!AppConfig.withCheckpoints)
  }

  test("Test 3. Checkpoints on when checkpoints are on") {
    AppConfig.withCheckpoints = true
    val actual = execute("checkpoints on")
    assert(AppConfig.withCheckpoints)
  }

  test("Test 4. Checkpoints without managing word") {
    val thrown = intercept[CustomException]{execute("checkpoints")}
    assert(thrown.getMessage().contains("Required argument(s) managing_word not found"))
  }

  test("Test 5. Checkpoints not last command") {
    val thrown = intercept[CustomException]{execute("eval a = 1 | checkpoints on | eval b = 2")}
    assert(thrown.getMessage().contains("Command checkpoints should be only one in query and last in query's commands list"))
  }

  test("Test 6. Checkpoints is last command, but checkpoints is repeating") {
    val thrown = intercept[CustomException] {
      execute("checkpoints on | eval a = 1 | eval b = 2 | checkpoints on")
    }
    assert(thrown.getMessage().contains("Command checkpoints should be only one in query and last in query's commands list"))
  }

  test("Test 7. Checkpoints with incorrect managing word") {
    val thrown = intercept[CustomException] {
      execute("eval a = 1 | checkpoints any")
    }
    assert(thrown.getMessage().contains("Required argument(s) managing_word not found"))
  }
}