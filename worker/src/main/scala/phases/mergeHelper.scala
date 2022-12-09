package phase

import java.io.File
import common.Utils
import java.nio.file.Files
import java.io.BufferedWriter
import java.io.FileWriter

object mergeHelper {
  def mergeFile(inputDirs: String, outputFilePath: String, id: Int, numSubrange: Int) = {
    Utils.createdir(outputFilePath)
    
    /*read all subrange directories*/
    var inputDirLists:Seq[String] = Seq()
    for(i <- 1 to numSubrange){
      val inputDir = inputDirs + "/subrange" + i.toString
      inputDirLists = inputDirLists:+inputDir
    }
    
    /*merge files into one in same subrange*/
    for (d <- inputDirLists){
      val files = Utils.getFilePathsFromDir(List(d))
      var partitions:Seq[Seq[String]] = Seq()
      for (f <- files){
        var items = Utils.getFile(f)
        partitions :+ items
      }

      /*make partition file names*/
      var number =  + 1
      var filenames:Seq[String] = Seq()
      for(i <-1 to numSubrange){
        val filename = outputFilePath + "/partition" + ((id-1) * numSubrange).toString
        filenames = filenames:+ filename
      }

      for (f <- filenames){
      val writer = new BufferedWriter(new java.io.FileWriter(f))
      for (line <- mergeSort(partitions)){
        writer.write(line)
      }
        writer.close()
      }
    }
  }
  
  def mergeSort(allpartitions: Seq[Seq[String]]):Seq[String] = 
    allpartitions match {
    case Nil => Nil
    case xs::Nil => xs
    case _ =>
      val (left, right) = allpartitions.splitAt(allpartitions.length/2)
      merge(mergeSort(left),mergeSort(right))
    }
    def merge(seq1: Seq[String], seq2: Seq[String]):Seq[String] = 
      (seq1,seq2) match {
        case (Nil,_) => seq2
        case (_,Nil) => seq1
        case (x::xs,y::ys) =>
          if((Utils.comparator(x,y))) x +: merge(xs,seq2)
          else y +: merge(seq1,ys)
      }
}
