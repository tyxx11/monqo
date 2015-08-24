package as.leap.monqo.test;

import as.leap.monqo.commons.MonqoException;
import as.leap.monqo.jobs.HbaseEtlJob;
import com.google.common.base.Throwables;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

/**
 * Created by willstan on 8/12/15.
 */
public class HbaseEtlTest {
	private String testFile;

	@Before
	public void prepare() throws IOException {
		System.setProperty("spark.master","local[*]");
		/*this.testFile = String.format("/tmp/spark-hbaseetl-%d.txt", System.currentTimeMillis());
		Path path = new Path(this.testFile);

		Configuration configuration = new Configuration();
		URL url = Thread.currentThread().getContextClassLoader().getResource("oplog.insert.txt");
		assert url != null;
		String bson = StringUtils.strip(IOUtils.toString(url));
		try (FileSystem fs = FileSystem.get(configuration); FSDataOutputStream os = fs.create(path)) {
			os.writeBytes(bson);
			os.flush();
		} catch (IOException e) {
			Throwables.propagate(e);
		}*/
	}

	@Test
	public void testWordCount() throws MonqoException {
		this.testFile = "/camus/kafka/OpLog/2015/08/18/1_54600";
		System.out.println("test mongo-hbase etl start!");
		HbaseEtlJob.main(new String[]{"hdfs://" + this.testFile});
	}

	/*@After
	public void cleanup() {
		try (FileSystem fs = FileSystem.get(new Configuration())) {
			fs.delete(new Path(this.testFile), false);
		} catch (IOException e) {
			Throwables.propagate(e);
		}
	}*/


}
