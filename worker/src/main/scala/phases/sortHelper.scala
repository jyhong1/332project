package phase

import scala.io.Source
import java.io.File
import common.Utils
import java.io.BufferedWriter
import java.io.FileWriter

/*
  sort helper function.
  read input blocks from inputDirs.
  get file and sort blocks and save the outputs in data/sort
*/
object  sortHelper {
  def sortSingleFile(inputFile: File, outputDir: String) = {
    val inputPath = inputFile.getPath()
    val inputItems = Source.fromFile(inputPath).getLines().toList
    val sortedItems = inputItems.sortWith((s1, s2) => Utils.comparator(s1, s2))
    Utils.createdir(outputDir)
    val outputPathString = outputDir + "/" + inputPath.split("/").last
    val writer = new BufferedWriter(new FileWriter(outputPathString))
    writer.flush()
    for (line <- sortedItems){
        writer.write(line+"\n")
    }
    writer.close()
  }

  def sort(inputDirs: List[String], outputDir:String) = {
    Utils.createdir(outputDir)
    val filePaths = Utils.getFilePathsFromDir(inputDirs)
    val files = filePaths.map { path =>
      val file = new File(path)
      file.createNewFile()
      file
    }
    var lines =
      for {
        file <- files
      } yield sortSingleFile(file,outputDir)
  }
}