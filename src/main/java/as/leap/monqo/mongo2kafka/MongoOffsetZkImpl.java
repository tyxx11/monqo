package as.leap.monqo.mongo2kafka;

import as.leap.monqo.jobs.ZkUtil;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by willstan on 8/20/15.
 */
public class MongoOffsetZkImpl implements MongoOffsetUtil {
	ZkUtil zkUtil;

	public MongoOffsetZkImpl(String zkServer) {
		zkUtil = new ZkUtil(zkServer);
	}

	@Override
	public int getOffset(String path) {
		return zkUtil.getMongoOffset(path);
	}

	@Override
	public void setOffset(String path, int offset) {
		zkUtil.setOffset(path,offset);
	}
}
