package common

import java.io.File
import scala.collection.mutable.Buffer
import scala.io.Source
import protos.network.RangeReply
import util.control.Breaks.{breakable,break}
import scala.reflect.io.Directory

object Utils {
  def comparator(s1: String, s2: String): Boolean = {
    s1.slice(0, 10) <= s2.slice(0, 10)
  }
  def comparatorWithout(s1: String, s2: String): Boolean = {
    s1.slice(0, 10) < s2.slice(0, 10)
  }

  def createdir (path:String) = {
    val dir = new File(path)
    if(!dir.exists()){
        dir.mkdir()
    }
  }

  def getFilePathsFromDir(dirs: List[String]): List[String] = {
    val filePaths = dirs.flatMap { dirPath =>
      val dir = new File(dirPath)
      val files = if (dir.exists() && dir.isDirectory()) {
        dir.listFiles().filter(_.isFile()).map(file => file.getPath()).toList
      } else {
        List[String]()
      }
      files
    }
    filePaths
  }

  def getFile(filename: String): Seq[String] = {
    var partition:Buffer[String]=Buffer()
    for (line <- Source.fromFile(filename).getLines())
    {
        partition = partition:+line
    }
    partition
  }

  //Get file list from single directory
  def getFileFromSingleDir(dirName: String): List[File] = {
    val dir = new File(dirName)
    if (dir.exists && dir.isDirectory) {
      dir.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  //Delete all files&dirs in single directory
  def deleteDir(dirName: String): Unit = {
    val dir = new Directory(new File(dirName))
    dir.deleteRecursively()
  }

  def getId(rangereply: RangeReply, localhostIP: String):Int = {
    var id = 0
    breakable{
      for (i <- 0 to rangereply.addresses.length-1){
        if(rangereply.addresses(i).ip == localhostIP){
          id = i
        }
     }
    }
    id
  }

  def waitWhile(condition: () => Boolean, timeout: Int): Boolean = {
    for (i <- 1 to timeout / 50)
      if (!condition()) return true else Thread.sleep(50)
        false
  }
  
  def convertToimmutable[T](sq: Seq[T]): scala.collection.Seq[T] = scala.collection.Seq[T](sq: _*)
  def convertTomutable[T](sq: scala.collection.Seq[T]): Seq[T] = Seq[T](sq: _*)
}

