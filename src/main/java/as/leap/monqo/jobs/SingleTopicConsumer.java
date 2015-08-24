package as.leap.monqo.jobs;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.fs.shell.Count;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by willstan on 8/17/15.
 */
public class SingleTopicConsumer implements Runnable {
	private List<String> _replicaBrokers = new ArrayList<>();
	private long lastOffset;
	private int _partition;
	private Queue<String> messages = new ConcurrentLinkedQueue<>();
	private Long _lastTime = null;
	private int sleepTimes = 0;
	private static String _topic;
	private static List<String> _seeds;
	// private static int _port;
	private static int fetchSize;
	private static ZkUtil _zk;
	private static Logger logger;
	private static WillsConsumerConfig conf;
	private static HdfsUtil hdfs;
	private static CountDownLatch countDownLatch;

	public SingleTopicConsumer(int partition) {
		logger.info("Initial Partition "+partition+" ...");
		_partition = partition;
		lastOffset =  _zk.getLastOffset("/hdfs/" + conf.groupId() + "/" + _topic + "/" + partition);
	}

	public SingleTopicConsumer(int partition, CountDownLatch countDownLatch) {
		logger.info("Initial Partition "+partition+" ...");
		_partition = partition;
		lastOffset =  _zk.getLastOffset("/hdfs/" + conf.groupId() + "/" + _topic + "/" + partition);
		this.countDownLatch = countDownLatch;
	}



	public static void main(String[] args) throws Exception {
		String path;
		if (args != null && args.length == 2) {
			_topic = args[0];
			path = args[1];
		}
		else {
			_topic = "OpLog";
			path = "hadoop/consumer.properties";
			//System.out.println("Invalid parameters");
			//System.exit(0);
		}
		conf = new WillsConsumerConfig(path);
		logger = LoggerFactory.getLogger(SingleTopicConsumer.class);
		_seeds = conf.brokers();
		// _port = conf.brokerPort();
		fetchSize = conf.fetchSize();
		_zk = new ZkUtil(conf.zkServer());
		hdfs = new HdfsUtil(conf.hdfs(), conf.job());
		List<String>partitions = _zk.getPartitions(_topic);
		CountDownLatch latch = new CountDownLatch(partitions.size());
		//创建线程池
        /*SingleTopicConsumer singleTopicConsumer = new SingleTopicConsumer(0);
        singleTopicConsumer.run();*/
		ExecutorService pool = Executors.newCachedThreadPool();
		for (String partition : partitions) {
			pool.execute(new Thread(new SingleTopicConsumer(Integer.parseInt(partition),latch)));
		}
		latch.await();
		logger.info("kafka-hdfs etl finished!");
	}

	public void run() {
		PartitionMetadata metadata = findLeader(_seeds, _topic, _partition);
		if (metadata == null) {logger.info("Can't find metadata for Topic and Partition. Exiting");return;}
		if (metadata.leader() == null) {logger.info("Can't find Leader for Topic and Partition. Exiting");return;}
		String leadBroker = metadata.leader().host();
		int leadPort = metadata.leader().port();
		String clientName = "Client_" + _topic + "_" + _partition;
		SimpleConsumer consumer = new SimpleConsumer(leadBroker, leadPort, 1000000, 64 * 1024, clientName);
		if (lastOffset == 0) {
			lastOffset = getLastOffset(consumer, _topic, _partition,kafka.api.OffsetRequest.LatestTime(), clientName);
		}
		int numErrors = 0;
		while (fetchSize>0) {
			if (consumer == null) {consumer = new SimpleConsumer(leadBroker, leadPort, 1000000, 64 * 1024, clientName);}
			// Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
			FetchRequest req = new FetchRequestBuilder().clientId(clientName).addFetch(_topic, _partition, lastOffset, fetchSize).build();
			FetchResponse fetchResponse = consumer.fetch(req);
			//something wrong
			if (fetchResponse.hasError()) {
				numErrors++;
				short code = fetchResponse.errorCode(_topic, _partition);
				logger.warn("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
				if (numErrors > 5) break;
				if (code == ErrorMapping.OffsetOutOfRangeCode()) {
					lastOffset = getLastOffset(consumer, _topic, _partition,kafka.api.OffsetRequest.LatestTime(), clientName);
					continue;
				}
				consumer.close();
				consumer = null;
				try {
					leadBroker = findNewLeader(leadBroker, _topic, _partition);
				} catch (Exception e) {
					logger.error("find new leader error! ",e);
				}
				continue;
			}
			numErrors = 0;
			long numRead = 0;
			for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(_topic, _partition)) {
				long currentOffset = messageAndOffset.offset();
				if (currentOffset < lastOffset) {
					logger.warn("Found an old offset: " + currentOffset + " Expecting: " + lastOffset);
					continue;
				}
				lastOffset = messageAndOffset.nextOffset();
				ByteBuffer payload = messageAndOffset.message().payload();
				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				String mes = null;
				try {
					mes = new String(bytes,"UTF-8");
				} catch (UnsupportedEncodingException e) {
					logger.error("encode error! ", e);
				}
				boolean inQueue = messages.offer(mes+'\n');
				Long currentTime = System.currentTimeMillis() / 1000;
				if (_lastTime==null || currentTime >= _lastTime + 30 || !inQueue) {
					_lastTime = currentTime;
					hdfsWriter();
				}
				numRead++;
				fetchSize--;
				sleepTimes = 0;
			}
			if (numRead == 0) {
				sleepTimes++;
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {}
				//if fetch no message in 60s, then clear the queue and reset the sleepTimes
				if (sleepTimes >= 30) {
					int queueSize = messages.size();
					if (queueSize > 0) {
						logger.info("..........." + _topic + " sleep too long with queue " + queueSize + "..............");
						hdfsWriter();
					}
					logger.info("======reset the sleepTimes======");
					break;
					//sleepTimes = 0;
				}
			}
		}
		countDownLatch.countDown();
		consumer.close();
	}
	public synchronized void hdfsWriter() {
		int queueSize = messages.size();
		try {
			String dateStr = DateFormatUtils.format(System.currentTimeMillis(),"yyyy/MM/dd",TimeZone.getTimeZone("Etc/UTC"));
			String hdfsPath = conf.hdfsTopicPath(_topic) + dateStr + "/" + _partition + "_" + lastOffset;
			hdfs.batchWrite(hdfsPath, messages);
		} catch (IOException e) {
			logger.error("hdfs write error! ", e);
		}
		_zk.setOffset("/hdfs/" + conf.groupId() + "/" + _topic + "/" + _partition, lastOffset);
		logger.info("..." + _topic + ":" + _partition + ":" + lastOffset + ":" + queueSize + "...");
	}

	public static long getLastOffset(SimpleConsumer consumer, String topic,int partition, long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic,partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(),clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);
		if (response.hasError()) {
			logger.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
			return 0;
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}

	private String findNewLeader(String a_oldLeader, String _topic,
								 int _partition) throws Exception {
		for (int i = 0; i < 3; i++) {
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeader(_replicaBrokers,
					_topic, _partition);
			if (metadata == null) {
				goToSleep = true;
			} else if (metadata.leader() == null) {
				goToSleep = true;
			} else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host())
					&& i == 0) {
				// first time through if the leader hasn't changed give
				// ZooKeeper a second to recover
				// second time, assume the broker did recover before failover,
				// or it was a non-Broker issue
				//
				goToSleep = true;
			} else {
				return metadata.leader().host();
			}
			if (goToSleep) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}
		logger.info("Unable to find new leader after Broker failure. Exiting");
		throw new Exception(
				"Unable to find new leader after Broker failure. Exiting");
	}

	private PartitionMetadata findLeader(List<String> a_seedBrokers, String _topic, int _partition) {
		PartitionMetadata returnMetaData = null;
		loop: for (String seed : a_seedBrokers) {
			SimpleConsumer consumer = null;
			try {
				int port = Integer.parseInt(seed.split(":")[1]);
				seed = seed.split(":")[0];
				consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024,"leaderLookup");
				List<String> topics = Collections.singletonList(_topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				TopicMetadataResponse resp = consumer.send(req);

				List<TopicMetadata> metaData = resp.topicsMetadata();
				for (TopicMetadata item : metaData) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == _partition) {
							returnMetaData = part;
							break loop;
						}
					}
				}
			} catch (Exception e) {
				logger.error("Error communicating with Broker [" + seed
						+ "] to find Leader for [" + _topic + ", "
						+ _partition + "] Reason: " + e);
			} finally {
				if (consumer != null)
					consumer.close();
			}
		}
		if (returnMetaData != null) {
			_replicaBrokers.clear();
			for (Broker replica : returnMetaData.replicas()) {
				_replicaBrokers.add(replica.host());
			}
		}
		return returnMetaData;
	}
}
