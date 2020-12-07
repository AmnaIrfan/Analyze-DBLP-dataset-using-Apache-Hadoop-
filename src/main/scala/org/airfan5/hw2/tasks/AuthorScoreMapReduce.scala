package org.airfan5.hw2.tasks

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.airfan5.hw2.util.{environment, helpers}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.slf4j.{Logger, LoggerFactory}
import scala.jdk.javaapi.CollectionConverters.asScala

/*
 * This map reduce task tackles the authorship score question of Assignment 2
 * 2) Next, you will produce the list of top 100 authors in the descending order
 * who publish with most co-authors and the list of 100 authors who publish with least co-authors.
 * To compute the authorship score you will use the following formula. The total score for a paper is one.
 * Hence, if an author published 15 papers as the single author without any co-authors, then her score will be 15.
 * For a paper with multiple co-authors the score will be computed using a split weight as the following.
 * First, each co-author receives 1/N score where N is the number of co-authors.
 * Then, the score of the last co-author is credited 1/(4N) leaving it 3N/4 of the original score.
 * The next co-author to the left is debited 1/(4N) and the process repeats until the first author is reached.
 * For example, for a single author the score is one. For two authors, the original score is 0.5.
 * Then, for the last author the score is updated 0.53/4 = 0.5-0.125 = 0.375 and the first author's score is 0.5+0.125 = 0.625.
 * The process repeats the same way for N co-authors.
 *
 * @author Amna Irfan
 */

object AuthorScoreMapReduce {

  def main(args: Array[String]): Unit = {

    //STEP 1 -> initialize the logger and configuration libraries
    val logger: Logger = LoggerFactory.getLogger(getClass)
    val configuration: Config = ConfigFactory.load("mapred.conf")
    logger.info("Configuration file has been loaded")

    //STEP 2 -> get environment
    val ENV = configuration.getString("authorScoreMapReduce.envName")

    //STEP 3 -> initialize the map reduce job
    val conf = new Configuration()
    val job = new Job(conf, "AuthorScoreMapReduce")

    //STEP 4 -> configure mapper and reducer classes
    job.setJarByClass(classOf[AuthorScoreMapper])
    job.setMapperClass(classOf[AuthorScoreMapper])
    job.setCombinerClass(classOf[AuthorScoreReducer])
    job.setReducerClass(classOf[AuthorScoreReducer])
    logger.info("Mapper and Reducer classes have been set")

    //STEP 5 -> set input and output type
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[DoubleWritable])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, DoubleWritable]])
    logger.info("Mapper and Reducer I/O types have been set")

    //STEP 6 -> set input and output director
    if (ENV == "") {
      logger.error("No environment specified")
      System.exit(0)
    }

    def getDirectories(): (String, String) = {
      environment.withName(ENV) match {
        case environment.LOCAL => {
          (configuration.getString("authorScoreMapReduce.inputDir"),configuration.getString("authorScoreMapReduce.outputDir"))
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

class AuthorScoreMapper extends Mapper[Object, Text, Text, DoubleWritable] {
  val mKey = new Text
  override def map(key:Object, value:Text, context:Mapper[Object,Text,Text,DoubleWritable]#Context) = {
    try
      {
        //extract the necessary nodes to get xml parsed data
        val data = helpers.parseDBLPXML(value.toString)
        val authors = data._3
        //get the publication score for each author for this specific paper
        val totalAuthors = authors.length
        //send in the number of authors there are, the function will return a list of distributed scores in reverse order
        val scores = helpers.getAuthorshipScore(totalAuthors)
        //loop through the index of each author and print a score for them
        0 to totalAuthors-1 foreach { i => (
          {
            //reversing totalAuthors while accessing it as scores are in reverse order
            mKey.set(authors(helpers.getReverseIndex(i, totalAuthors)).text.toString)
            context.write(mKey, new DoubleWritable(scores(i)))
          }
        )}
      }
    catch
      {
        case e: Throwable => println(e.printStackTrace())
      }
  }
}

class AuthorScoreReducer extends Reducer[Text,DoubleWritable,Text,DoubleWritable] {
  override def reduce(key: Text, values: java.lang.Iterable[DoubleWritable], context:Reducer[Text, DoubleWritable, Text, DoubleWritable]#Context) = {
    //sum all scores the author received from different papers
    val sum = asScala(values).foldLeft(0.0) { (t,i) => t + i.get }
    context.write(key, new DoubleWritable(sum))
  }
}
