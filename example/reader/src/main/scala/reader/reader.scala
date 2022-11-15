import scala.io.Source
import common.Log


class reader {
  Log.info("Reader start")
  def read() : Unit = {
    val fileName = "text.txt"
    val filePath = "./././"
    val fileLocation = filePath + fileName
    val Storage = Array[String]()
    val fileSource = Source.fromFile(fileLocation)
    while (fileSource.hasNext){
        Storage :+ fileSource.next
    }
    Log.info(Storage)   
    fileSource.close() 
  }  
  
}