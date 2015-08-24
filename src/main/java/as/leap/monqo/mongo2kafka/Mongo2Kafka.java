package as.leap.monqo.mongo2kafka;

import as.leap.monqo.etl.model.Oplog;
import as.leap.monqo.jobs.WillsConsumerConfig;
import as.leap.monqo.jobs.ZkUtil;
import as.leap.monqo.parquet.helper.PandoraHelper;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.collections.MapUtils;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * Created by willstan on 8/19/15.
 */
public class Mongo2Kafka {
	static Logger LOG = LoggerFactory.getLogger(Mongo2Kafka.class);
	static MongoOffsetUtil mongoOffsetTool;
	static WillsConsumerConfig config;
	static String topic;
	static Producer<String,String> producer;
	final static String mongoDb = "local";
	final static String mongoTable = "oplog.rs";

	public Mongo2Kafka(){
	    //TODO
	}

	public static void main(String[] args){
		String path;
		if (args != null && args.length == 2) {
			path = args[1];
			topic = args[0];
		}
		else {
			path = "hadoop/consumer.properties";
			topic = "Oplog";
		}
		config = new WillsConsumerConfig(path);
		initConfig();
		//use zookeeper offsetImpl
		mongoOffsetTool = new MongoOffsetZkImpl(config.zkServer());
		int timestamp = mongoOffsetTool.getOffset(config.getMongoZkPath());
		LOG.info("timetamp! : " + timestamp);
		Iterable<String> mongoHostList = PandoraHelper.getMongoHosts(mongoDb, mongoTable);
		List<ServerAddress> seeds = new ArrayList<>();
		for(String mongoHosts : mongoHostList){
			String[] mongHost = mongoHosts.split(":");
			String host = mongHost[0];
			int port = Integer.parseInt(mongHost[1]);
			ServerAddress serverAddress = new ServerAddress(host,port);
			seeds.add(serverAddress);
		}
		MongoClient mongoClient = new MongoClient(seeds);
		BsonTimestamp bsonTs;
		if (timestamp == 0){
			//如果没有offset信息，那么从当前时刻开始读oplog
			bsonTs = new BsonTimestamp((int) (new Date().getTime() / 1000 - 36000), 0);
		}else {
			bsonTs = new BsonTimestamp(timestamp,0);
		}
		MongoCollection collection = mongoClient.getDatabase("local").getCollection("oplog.rs");
		Document ts = new Document("ts", new Document("$gte", bsonTs));

		FindIterable findIterable = collection.find(ts).cursorType(CursorType.Tailable);
		findIterable.forEach(new Consumer() {
			@Override
			public void accept(Object o) {
				Document doc = (Document) o;
				Oplog oplog = Oplog.valueOf(doc.toJson());
				int ts = Integer.parseInt(MapUtils.getMap(oplog.getTimestamp(), "$timestamp").get("t").toString());
				KeyedMessage<String,String> keyedMessage = new KeyedMessage<>(topic,doc.toJson());
				producer.send(keyedMessage);
				mongoOffsetTool.setOffset(config.getMongoZkPath(), ts);
				LOG.info("send mes successed and offset updated!...");
			}
		});
	}

	public static void initConfig(){
		Properties props = new Properties();
		props.put("metadata.broker.list", config.brokerListStr());
		props.put("request.required.acks", "1");
		props.put("request.timeout.ms", "10000");
		//配置value的序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		//配置key的序列化类
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<>(config);
	}
}
