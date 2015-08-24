package as.leap.monqo.jobs

import as.leap.monqo.commons.MonqoRuntimeException
import as.leap.monqo.etl.impl.ETLServiceImpl
import as.leap.monqo.parquet.helper.PandoraHelper
import as.leap.monqo.parquet.schema.{Schema, SchemaProvider}
import com.google.common.base.Joiner
import com.mongodb.hadoop.MongoInputFormat
import org.apache.commons.cli._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.BSONObject

/**
 * class description
 * <p>add some remarks.</p>
 * author: jeff.tsai
 * date: 2015-08-14
 * email: jjeffcaii@outlook.com
 */
object CloudDataSync {


  def main(args: Array[String]) {


    val options = new Options()
      .addOption("i", null, true, "CloudData AppID")
      .addOption("c", null, true, "CloudData ClassName (optional)")

    val helper = new HelpFormatter()
    val parser: CommandLineParser = new PosixParser()
    var cl: CommandLine = null
    val formatString: String = "PROGRAM -i APP_ID [-c CLASS_NAME] --help"
    try {
      cl = parser.parse(options, args)
    } catch {
      case e: ParseException => helper.printHelp(formatString, options)
    }

    if (cl.hasOption("help") || !cl.hasOption("i")) {
      helper.printHelp(formatString, options)
      return
    }

    val database = cl.getOptionValue("i")

    val sc = new SparkContext(new SparkConf().setAppName("monqo-clouddata-sync"))

    if (!cl.hasOption("c")) {
      val schemas = SchemaProvider.DEFAULT_INSTANCE.loadAll(database).iterator()
      while (schemas.hasNext) {
        sync(sc, schemas.next())
      }
    } else {
      val alias = cl.getOptionValue("c")
      val next = SchemaProvider.DEFAULT_INSTANCE.loadByAlias(database, alias).orNull()
      if (next == null) {
        throw new MonqoRuntimeException(s"no such schema: database=$database,className=$alias")
      }
      sync(sc, next)
    }
    sc.stop()
  }

  def sync(sc: SparkContext, schema: Schema): Unit = {
    val mongoHosts = Joiner.on(",").join(PandoraHelper.getMongoHosts(schema.getDatabase, schema.getCollection))
    val mongoDatabase = schema.getDatabase
    val mongoTable = schema.getCollection

    println("============================")
    println(s"SYNC: $mongoDatabase.$mongoTable")
    println("============================")

    val hconf = new Configuration()
    hconf.set("mongo.input.uri", s"mongodb://$mongoHosts/$mongoDatabase.$mongoTable")
    sc.newAPIHadoopRDD(hconf, classOf[MongoInputFormat], classOf[Object], classOf[BSONObject])
      .foreachPartition {
      case f => {
        val etl = new ETLServiceImpl()
        f.foreach(v => etl.process(mongoDatabase, mongoTable, v._2))
      }
    }


  }

}
