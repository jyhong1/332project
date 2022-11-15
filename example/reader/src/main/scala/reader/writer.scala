import scala.io.Source
import common.Log
import java.io.File
import java.io.PrintWriter

class writer {
  Log.info("Writer start")

  def write() :Unit ={
    val fileObject = new File("test.txt")
    val printWriter = new PrintWriter(fileObject)
    val contents = "Hello, World!"
    printWriter.write(contents)
    printWriter.close()
    Log.info("Writer Done!")
  }

}
