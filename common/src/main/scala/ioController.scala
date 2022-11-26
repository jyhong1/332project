package gensort.common

import scala.collection.mutable
import java.io.{File, IOException}

object ioController {
    def getFiles(directoryPath: String): List[File] = {
        val dir = new File(directoryPath)
        if (contents.exists && contents.isDirectory){
            contents.listFiles.filter(_.isFile).toList
        }else{
            List[File]()
        }
    }
}
