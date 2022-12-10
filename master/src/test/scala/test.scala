import org.scalatest.funsuite._
import common.Utils

class HelloSpec extends AnyFunSuite {
  test("Hello should start with H") {
    assert("Hello".startsWith("H"))
  }

  test("Test deleting dir") {
    val dirPath = System.getProperty("user.dir") + "/test_dir"
    Utils.deleteDir(dirPath)
  }
}
