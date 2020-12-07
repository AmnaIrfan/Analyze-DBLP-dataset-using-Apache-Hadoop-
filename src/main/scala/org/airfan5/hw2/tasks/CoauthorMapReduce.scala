package org.airfan5.hw2.tasks

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}
import org.airfan5.hw2.util.{environment, helpers}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable.ListBuffer

/*
 * This map reduce task tackles the fourth question of Assignment 2
 * 4. Finally, you will output a spreadsheet where for each author you will compute the max, median, and the average number of authors
 * for publication on which the name of the author appears.
 *
 * @author Amna Irfan
 */
object CoauthorMapReduce {
  def main(args: Array[String]): Unit = {

    //STEP 1 -> initialize the logger and configuration libraries
    val logger: Logger = LoggerFactory.getLogger(getClass)
    val configuration: Config = ConfigFactory.load("mapred.conf")
    logger.info("Configuration file has been loaded")

    //STEP 2 -> get environment
    val ENV = configuration.getString("coauthorMapReduce.envName")

    //STEP 3 -> initialize the map reduce job
    val conf = new Configuration()
    val job = new Job(conf, "CoauthorMapReduce")

    //STEP 4 -> configure mapper and reducer classes
    job.setJarByClass(classOf[CoauthorMapper])
    job.setMapperClass(classOf[CoauthorMapper])
    job.setReducerClass(classOf[CoauthorMapReducer])
    logger.info("Mapper and Reducer classes have been set")

    //STEP 5 -> set input and output type
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
    logger.info("Mapper and Reducer I/O types have been set")

    //STEP 6 -> set input and output director
    if (ENV == "") {
      logger.error("No environment specified")
      System.exit(0)
    }

    def getDirectories(): (String, String) = {
      environment.withName(ENV) match {
        case environment.LOCAL => {
          (configuration.getString("coauthorMapReduce.inputDir"),configuration.getString("coauthorMapReduce.outputDir"))
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

class CoauthorMapper extends Mapper[Object, Text, Text, IntWritable] {
  val mKey = new Text
  override def map(key:Object, value:Text, context:Mapper[Object,Text,Text,IntWritable]#Context) = {
    try
      {
        //extract the necessary nodes to get xml parsed data
        val data = helpers.parseDBLPXML(value.toString)
        val authors = data._3
        val totalAuthors = new IntWritable(authors.length)
        //each author will get the same value as they all are coauthors of the paper
        authors.foreach((a) => {
          mKey.set(a.text.toString)
          //we can't ignore a single author case as the author's name still needs to be displayed on the spreadsheet with 1
            context.write(mKey, totalAuthors)
        })
      }
    catch
      {
        case e: Throwable => println(e.printStackTrace())
      }
  }
}

class CoauthorMapReducer extends Reducer[Text,IntWritable,Text,Text] {
  val stats = new Text()
  override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context:Reducer[Text,IntWritable,Text,Text]#Context) = {
    var coauthors = new ListBuffer[IntWritable]()
    //adding to a buffer since looping through an interable is just a pointer which makes empties the list after looping
    values.forEach((v) => {
      coauthors += new IntWritable(v.get())
    })
    //calculate max, median, mean and concatenate them together as Text with a pipe
    val median = helpers.getMedian(coauthors)
    val mean = helpers.getMean(coauthors)
    //composed text value with all stats concatenated (max, median, mean)
    stats.set(coauthors.max + "|" + median + "|" + mean)
    context.write(key, stats)
  }
}

