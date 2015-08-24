package as.leap.monqo.test;

import as.leap.monqo.commons.Utils;
import as.leap.monqo.etl.model.Oplog;
import as.leap.monqo.parquet.helper.PandoraHelper;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by willstan on 8/19/15.
 */
public class MongoEtlTest {
	@Test
	public void test(){
		System.out.println(PandoraHelper.getMongoHosts("local", "oplog.rs"));
		MongoClient mongoClient = new MongoClient("3.default.mgo.las", 27017);
		MongoCollection collection = mongoClient.getDatabase("local").getCollection("oplog.rs");
		Document ts = new Document("ts", new Document("$gte", new BsonTimestamp((int) (new Date().getTime() / 1000 - 36000), 0)));

		FindIterable findIterable = collection.find(ts).cursorType(CursorType.Tailable);
		findIterable.forEach((Consumer) o -> System.out.println(o));

	}
	@Test
	public void testt(){
		System.out.println(System.currentTimeMillis()/1000);
	}


	@Test
	public void ttt(){
		String docStr = "{ \"ts\" : { \"$timestamp\" : { \"t\" : 1439977669, \"i\" : 1 } }, \"h\" : { \"$numberLong\" : \"7133740925574316862\" }, \"v\" : 2, \"op\" : \"i\", \"ns\" : \"55d3feef5ed20101d7013e5e.sys_file_l_55d3feef5ed20101d7013e5e\", \"o\" : { \"_id\" : { \"$oid\" : \"55d450c760b2e4dfd82bd6cb\" }, \"name\" : \"zcf-3762efbb-5bd1-4cf7-8bf1-6e9f61b74726.txt\", \"time\" : { \"$numberLong\" : \"1439977670809\" }, \"size\" : 0, \"region\" : \"us-east-1\", \"url\" : \"cfdev.appfra.com/AtvkejSt0cQ2QCwTTmiVXg/zcf-3762efbb-5bd1-4cf7-8bf1-6e9f61b74726.txt\", \"createdAt\" : { \"$numberLong\" : \"1439977671678\" }, \"updatedAt\" : { \"$numberLong\" : \"1439977671678\" } } }\n";
		Document doc = Document.parse(docStr);
		Oplog oplog = Oplog.valueOf(docStr);
		System.out.println(oplog);
	}
}
