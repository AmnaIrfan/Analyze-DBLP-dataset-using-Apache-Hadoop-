package org.airfan5.hw2.util

/*
* This class enumerates the type of environment the map reduce jar file is being run on. For example, if local the
* map reduce framework will use the input and output directory specified in the configuration file. If not, it will use the
* arguments to determine directories.
*
* @author Amna Irfan
*/

object environment extends Enumeration {
  val LOCAL = Value("local")
  val HADOOP = Value("hadoop")
  val EMR = Value("emr")

  def name(s: String): Option[Value] = values.find(_.toString == s)
}
