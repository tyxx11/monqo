package as.leap.monqo.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Queue;

/**
 * Created by willstan on 8/17/15.
 */
public class HdfsUtil {
    private String hdfsURL;
    private Configuration conf;

    public HdfsUtil(String hdfs, Configuration config){
        this.hdfsURL = hdfs;
        this.conf = config;
    }
    public void write(String file, String content) throws IOException {
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        FileSystem fs = FileSystem.get(URI.create(hdfsURL), conf);
        try {
            //追加
            os = fs.append(new Path(file));
        } catch (FileNotFoundException e) {
            os = fs.create(new Path(file));
        } finally {
            os.write(buff, 0, buff.length);
            if (os != null)
                os.close();
        }
        fs.close();
    }
    public void batchWrite(String file,Queue<String>messages) throws IOException {
        String mes;
        FSDataOutputStream os;
        FileSystem fs = FileSystem.get(URI.create(hdfsURL), conf);
        os = fs.create(new Path(file));
        while ((mes = messages.poll()) != null) {
			byte[] buff = mes.getBytes();
			os.write(buff, 0, buff.length);
		}
        if (os != null)
			os.close();

    }

    public void mvToHistory(String hisPath, String currentPath) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsURL),conf);
        fs.rename(new Path(currentPath), new Path(hisPath));
        fs.close();
    }
}