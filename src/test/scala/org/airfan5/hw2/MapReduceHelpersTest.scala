package org.airfan5.hw2

import com.typesafe.scalalogging.LazyLogging
import org.airfan5.hw2.util.helpers
import org.apache.hadoop.io.IntWritable
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import scala.collection.mutable.ListBuffer

/*
* This class consists of five unit tests. Each unit test corresponds to a method defined in the helper class
* of the utility package.
*
* @author Amna Irfan
*/

class MapReduceHelpersTest extends FunSuite with BeforeAndAfterEach with LazyLogging {

  //TestGetMedian gets the median of a list of integers
  test("helpers.getMedian\n") {
    logger.info("Verifying the get median method of util\n")
    var list = new ListBuffer[IntWritable]()
    list += new IntWritable(2)
    list += new IntWritable(1)
    val median = helpers.getMedian(list)
    assert(median == 1.5 )
  }
  //TestGetMedian gets the mean of a list of integers
  test("helpers.getMean\n") {
    logger.info("Verifying the get mean method of util\n")
    var list = new ListBuffer[IntWritable]()
    list += new IntWritable(2)
    list += new IntWritable(1)
    list += new IntWritable(5)
    val mean = helpers.getMean(list)
    assert(mean == 2.67 )
  }
  //TestParseDBLPXML uses the DTD definition of the DBLP xml record and sends back parsed authors, name of venue and year.
  test("helpers.parseDBLPXML\n") {
    logger.info("Verifying that the DBLP xml record is parsed correctly\n")
    val data = helpers.parseDBLPXML("<inproceedings mdate=\"2017-05-21\" key=\"conf/bmvc/LeottaM06\"><author>Matthew J. Leotta</author><author>Joseph L. Mundy</author><title>Learning Background and Shadow Appearance with 3-D Vehicle Models.</title><pages>649-658</pages><year>2006</year><booktitle>BMVC</booktitle><ee>https://doi.org/10.5244/C.20.67</ee><crossref>conf/bmvc/2006</crossref><url>db/conf/bmvc/bmvc2006.html#LeottaM06</url></inproceedings>")
    val venue = data._1
    val year = data._2
    val authors = data._3
    assert(venue == "inproceedings" & authors.length == 2 && year.text == "2006")
  }

  //TestGetAuthorship score verifies the distribution of the author score given the number of authors for a certain paper/
  test("helpers.getAuthorScore\n") {
    logger.info("Verifying the distribution of the author score for a single paper that has 2 authors\n")
    val scores = helpers.getAuthorshipScore(2)
    assert(scores.length == 2 && scores(0) == 0.375 && scores(1) == 0.625)
  }

  //TestGetReverseIndex verifies the reversed index corresponds to the same value if the array was reversed
  test("helpers.getReverseIndex\n") {
    logger.info("Verifying the reversed index corresponds to the same value if the array was reversed\n")
    val xList = List("garlic", "tomatoes", "spinach", "avocado", "parsley", "lettuce", "potatoes")
    val index = helpers.getReverseIndex(3, xList.length)
    assert(xList.reverse(3) == xList(index))
  }
}
