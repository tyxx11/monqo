package as.leap.monqo.jobs

import as.leap.monqo.commons.Utils
import as.leap.monqo.etl.hexec.HExec
import as.leap.monqo.parquet.MonqoParquetWriter
import as.leap.monqo.parquet.hbase.HRecord
import as.leap.monqo.parquet.schema.SchemaProvider
import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
 * class description
 * <p>add some remarks.</p>
 * author: jeff.tsai
 * date: 2015-08-12
 * email: jjeffcaii@outlook.com
 */
object HBase2Parquet {

  def main(args: Array[String]) {
    val sparkContext = new SparkContext(new SparkConf().setAppName("hbase-to-parquet"))

    val hConf = HBaseConfiguration.create
    val props = Utils.configuration("monqo.etl.properties")
    hConf.set("hbase.zookeeper.property.clientPort", props.getString("hbase.zookeeper.property.clientPort"))
    hConf.set("hbase.zookeeper.quorum", props.getString("hbase.zookeeper.quorum"))
    hConf.set("hbase.master", props.getString("hbase.master"))
    hConf.set(TableInputFormat.INPUT_TABLE, props.getString("hbase.tableName"))
    val hrdd = sparkContext.newAPIHadoopRDD(hConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    hrdd.map {
      case (a, b) => {
        val triple = HExec.analyzeRowKey(a.copyBytes())
        ((triple.getLeft, triple.getMiddle), new HRecord(b))
      }
    }.groupByKey().foreach {
      case (a, b) => {
        val warehouse = Utils.configuration("monqo.parquet.properties").getString("monqo.parquet.warehouse")

        val schema = SchemaProvider.DEFAULT_INSTANCE.load(a._1, a._2)

        if (schema.isPresent) {
          val database = schema.get().getDatabase
          val alias = schema.get().getAlias
          val mod = (System.currentTimeMillis() / DateUtils.MILLIS_PER_DAY) % 2
          val path = new Path(s"$warehouse/$database/$alias/$mod")
          val writer = new MonqoParquetWriter(schema.get(), path, true)
          b.foreach(writer.write)
          writer.close()
        }
      }
    }
    sparkContext.stop()
  }

}
