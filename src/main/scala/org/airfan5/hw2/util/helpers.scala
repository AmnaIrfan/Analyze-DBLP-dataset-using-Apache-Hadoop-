package org.airfan5.hw2.util
import org.apache.hadoop.io.IntWritable
import scala.collection.mutable.ListBuffer
import scala.xml.{NodeSeq, XML}

/*
* This class has helper methods that are used by the map reduce jobs. Most of these helper methods are used by
* multiple map reduce jobs. They all have corresponding tests in the test package.
*
* @author Amna Irfan
*/

object helpers {
  //gets the median of a list of integers
  def getMedian(values: ListBuffer[IntWritable]): Double = {
    val v = values.sorted
    if (v.length % 2 == 0) {
      val v1 = v(v.length/2 - 1).get()
      val v2 = v(v.length/2).get()
      return  BigDecimal(((v1 + v2) / 2.0).toFloat).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    }
    v(v.length/2).get().toFloat
  }

  //gets the mean of a list of integers
  def getMean(values: ListBuffer[IntWritable]): Double = {
    val sum = values.foldLeft(0) { (t,i) => t + i.get }
    (BigDecimal(sum/values.length.toFloat).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
  }

  //parses the dblp xml record using it's dtd
  def parseDBLPXML(xml: String): (String, NodeSeq, NodeSeq ) = {
    //load the xml file along with the DTD
    val pub = XML.loadString("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n<!DOCTYPE dblp SYSTEM \""+getClass.getClassLoader.getResource("dblp.dtd").toURI.toString+"\">"+xml.toString())
    (pub.head.label, pub\"year", pub\"author" )
  }

  //gets the reverse index of a list
  def getReverseIndex(index: Int, total: Int): Int  = {
    return(total-1) - index
  }

  //gets distribution of the authorship score for a single paper
  def getAuthorshipScore(authorNum: Int): ListBuffer[Double]= {
    var scores = new ListBuffer[Double]()
    val baseScore = 1/authorNum.toDouble
    if (authorNum == 1){
      return scores += 1.0
    }
    0 to authorNum-1 foreach { x => (
      if (x == 0){
        scores += 0.75 * baseScore
      } else {
        val newScore = baseScore + (0.25 * (scores(x-1)/0.75))
        if (x == authorNum-1) {
          scores += newScore
        } else {
          scores += 0.75 * newScore
        }
      }
      )}
    return (scores)
  }
}
