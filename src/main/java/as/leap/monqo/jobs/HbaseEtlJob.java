package as.leap.monqo.jobs;

import as.leap.monqo.commons.MonqoException;
import as.leap.monqo.commons.Utils;
import as.leap.monqo.etl.ETLService;
import as.leap.monqo.etl.impl.ETLServiceImpl;
import as.leap.monqo.etl.model.Oplog;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by willstan on 8/12/15.
 */
public class HbaseEtlJob {
	public static void main(String[] args) throws MonqoException {

		JavaSparkContext context = new JavaSparkContext(new SparkConf().setAppName("monqo-etl"));

		JavaRDD<String> textFile = context.textFile(args[0]);

		textFile.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				if (s != null && !"".equals(s)) {
					Oplog oplog = Utils.fromJson(s, Oplog.class);
					String dbTableKey = oplog.getNamespace();
					return new Tuple2<>(dbTableKey, s);
				} else {
					return new Tuple2<>(null, null);
				}
			}
		}).groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Long, String>() {
			@Override
			public Tuple2<Long, String> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
				for (String s : stringIterableTuple2._2()) {
					if (s != null && !"".equals(s)) {
						Oplog oplog = Utils.fromJson(s, Oplog.class);
						long timestamp = Long.parseLong(MapUtils.getMap(oplog.getTimestamp(), "$timestamp").get("t").toString());
						return new Tuple2<>(timestamp, s);
					}
				}
				return new Tuple2<>(0L, null);
			}
		}).sortByKey().foreach(longStringTuple2 -> {
			if (longStringTuple2._1() != 0L) {
				final ETLService service = new ETLServiceImpl();
				service.process(longStringTuple2._2());
				System.out.println(longStringTuple2._1() + ":" + longStringTuple2._2());
			}
		});


		context.stop();
	}
}
