package as.leap.monqo.jobs

import as.leap.monqo.etl.impl.ETLServiceImpl
import com.mongodb.hadoop.MongoInputFormat
import org.apache.commons.cli._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.BSONObject

/**
 * class description
 * <p>add some remarks.</p>
 * author: jeff.tsai
 * date: 2015-08-13
 * email: jjeffcaii@outlook.com
 */
object SimpleMongoSync {

  def main(args: Array[String]) {
    val options = new Options()
      .addOption("h", "host", true, "mongodb host")
      .addOption("p", "port", true, "mongodb port")
      .addOption("d", "database", true, "mongodb database")
      .addOption("t", "table", true, "mongodb table")

    val helper = new HelpFormatter()
    val parser: CommandLineParser = new PosixParser()
    var cl: CommandLine = null
    val formatString: String = "PROGRAM [--host/-h] HOST [-p/--port] PORT [--database/-d] DB [--table/-t] TABLE"
    try {
      cl = parser.parse(options, args)
    } catch {
      case e: ParseException => helper.printHelp(formatString, options)
    }

    if (cl.hasOption("help")) {
      helper.printHelp(formatString, options)
      return
    }

    if (!cl.hasOption("d") || !cl.hasOption("t")) {
      return
    }

    val mongoHost = cl.getOptionValue("h", "127.0.0.1")
    val mongoPort = cl.getOptionValue("p", "27017").toInt
    val mongoDatabase = cl.getOptionValue("d")
    val mongoTable = cl.getOptionValue("t")

    val hconf = new Configuration()
    hconf.set("mongo.input.uri", s"mongodb://$mongoHost:$mongoPort/$mongoDatabase.$mongoTable")
    val sparkContext = new SparkContext(new SparkConf().setAppName("monqo-mongo-sync"))
    sparkContext.newAPIHadoopRDD(hconf, classOf[MongoInputFormat], classOf[Object], classOf[BSONObject])
      .foreachPartition {
      case f => {
        val etl = new ETLServiceImpl()
        f.foreach(v => etl.process(mongoDatabase, mongoTable, v._2))
      }
    }
    sparkContext.stop()
  }

}
