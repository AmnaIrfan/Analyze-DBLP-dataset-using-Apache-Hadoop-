package org.airfan5.hw2.tasks

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.slf4j.{Logger, LoggerFactory}
import scala.jdk.javaapi.CollectionConverters.asScala
import org.airfan5.hw2.util.{environment, helpers}

/*
 * This map reduce task tackles the first three question of Assignment 2
 * 1. One such statistics can be expressed as a histogram where each bin shows the range of the numbers of co-authors (e.g.,
 * the first bin is one, second bin is 2-3, third is 4-6, and so on until the max number of co-authors).
 * 2 & 3. The other statistics will produce the histogram stratified by journals, conferences, and years of publications.
 *
 * @author Amna Irfan
 */


object PublicationMapReduce {

  def main(args: Array[String]): Unit = {

    //STEP 1 -> initialize the logger and configuration libraries
    val logger: Logger = LoggerFactory.getLogger(getClass)
    val configuration: Config = ConfigFactory.load("mapred.conf")
    logger.info("Configuration file has been loaded")

    //STEP 2 -> get environment
    val ENV = configuration.getString("publicationMapReduce.envName")

    //STEP 3 -> initialize the map reduce job
    val conf = new Configuration()
    val job = new Job(conf, "PublicationMapReduce")

    //STEP 4 -> configure mapper and reducer classes
    job.setJarByClass(classOf[PublicationMapper])
    job.setMapperClass(classOf[PublicationMapper])
    job.setCombinerClass(classOf[PublicationReducer])
    job.setReducerClass(classOf[PublicationReducer])
    logger.info("Mapper and Reducer classes have been set")

    //STEP 5 -> set input and output type
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    logger.info("Mapper and Reducer I/O types have been set")

    //STEP 6 -> set input and output director
    if (ENV == "") {
      logger.error("No environment specified")
      System.exit(0)
    }

    def getDirectories(): (String, String) = {
      environment.withName(ENV) match {
        case environment.LOCAL => {
          (configuration.getString("publicationMapReduce.inputDir"),configuration.getString("publicationMapReduce.outputDir"))
        }
        case environment.HADOOP => {
          (args(1), args(2))
        }
        case environment.EMR => {
          (args(0), args(1))
        }
      }
    }

    val dir = getDirectories()
    if (dir._1 == "") {
      logger.error("No input directory specified")
      System.exit(0)
    }
    if (dir._2 == "") {
      logger.error("No output directory specified")
      System.exit(0)
    }
    FileUtils.deleteDirectory(new File(dir._2))
    FileInputFormat.addInputPath(job, new Path(dir._1))
    FileOutputFormat.setOutputPath(job, new Path(dir._2))

    logger.info("I/O directories have been set. Job is running now ...")
    logger.info("Input Dir: " + dir._1)
    logger.info("Output Dir: " + dir._2)

    //STEP 7 -> start the job
    if (job.waitForCompletion(false)) {
      logger.info("Map/Reduce COMPLETED. Check results in Output Dir: "+ dir._2)
    } else {
      logger.error("Map/Reduce FAILED")
    }
  }
}

class PublicationMapper extends Mapper[Object, Text, Text, IntWritable] {
  val one = new IntWritable(1)
  val mKey = new Text
  override def map(key:Object, value:Text, context:Mapper[Object,Text,Text,IntWritable]#Context) = {
    try
      {
        //extract the necessary nodes to get xml parsed data
        val data = helpers.parseDBLPXML(value.toString)
        val venue = data._1
        val year = data._2
        val authors = data._3
        //create a composite key (year, venue, number of authors)
        mKey.set(year.text + "|" + venue + "|" + authors.length)
        context.write(mKey, one)
      }
    catch
      {
        case e: Throwable => println(e.printStackTrace())
      }
  }
}

class PublicationReducer extends Reducer[Text,IntWritable,Text,IntWritable] {
  override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context:Reducer[Text, IntWritable, Text, IntWritable]#Context) = {
    //sum all publications with the same composite key
    val sum = asScala(values).foldLeft(0) { (t,i) => t + i.get }
    context.write(key, new IntWritable(sum))
  }
}