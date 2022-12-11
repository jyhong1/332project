package phase

import scala.io.Source
import java.io.File
import common.Utils
import java.io.BufferedWriter
import java.io.FileWriter

/*
  partitionMaker makes partition files named 
  "TO_(receving worker id)_From_(sender worker id)_(input block index)" in the data/partitions
*/

object partitionMaker {
  def findRange(item: String, from: String, to: String) = {
    var isInRange = false
    if (Utils.comparatorWithout(from,item) && Utils.comparator(item, to)){
        isInRange = true
    }
    isInRange
  }

  def writeItems(items: Seq[String], outputDir: String, from: Int, to: Int, count: Int) = {
    Utils.createdir(outputDir)
    val filename = outputDir + "/To_" + to.toString() + "_From_" + from.toString() + "_" + count.toString
    val writer = new BufferedWriter(new FileWriter(filename))
    writer.flush()
    for (line <- items){
        writer.write(line+"\n")
    }
    writer.close()
  }

  def partitionSingleFile (
    inputItems: Seq[String],
    outputDir: String,
    ranges: Seq[protos.network.Range],
    id: Int,
    inputCount: Int
    ) = {
  
    val numItems = inputItems.length
    val partitionSizeLimit = 10000
    var rangeIndex = 0
    
    def divideItems (item: Seq[String], index: Int):Unit = {
        // if the item is not in the range
        if (findRange(item(index),ranges(rangeIndex).from,ranges(rangeIndex).to) == false){
            val (head,tail) = item.splitAt(index)
            writeItems(head,outputDir,id,rangeIndex+1,inputCount)
            rangeIndex += 1
            divideItems(tail,0)
        }
        //end of file
        else if(index == item.length-1){
            writeItems(item,outputDir,id,rangeIndex+1,inputCount)
        }
        // continue 
        else{
          divideItems(item,index+1)
        }
    }
    divideItems(inputItems,0)
  }

  def partition(
    inputDir: String,
    outputDir: String,
    ranges: Seq[protos.network.Range],
    id: Int
    ) = { 
    val filePaths = Utils.getFilePathsFromDir(List(inputDir))
    Utils.createdir(outputDir)
    for( i <- 0 to filePaths.length -1){
        val sorteditems = Source.fromFile(filePaths(i)).getLines.toList
        partitionSingleFile(sorteditems, outputDir, ranges, id+1,i)
    }

  }
}
