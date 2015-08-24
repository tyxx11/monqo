package as.leap.monqo.mongo2kafka;

/**
 * Created by willstan on 8/20/15.
 */
public interface MongoOffsetUtil {
	public int getOffset(String path);
	public void setOffset(String path, int offset);
}
