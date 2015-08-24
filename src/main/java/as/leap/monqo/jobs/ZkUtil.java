package as.leap.monqo.jobs;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by willstan on 8/17/15.
 */
public class ZkUtil {
    private static ZooKeeper zk;
    Logger LOG = LoggerFactory.getLogger(ZkUtil.class);

    public ZkUtil(String zkserver) {
        // 创建一个与服务器的连接
        try {
            zk = new ZooKeeper(zkserver, 60000, new Watcher() {
                // 监控所有被触发的事件
                public void process(WatchedEvent event) {
                    //System.out.println("EVENT:" + event.getType());
                    //System.out.println("...");
                }
            });
        } catch (IOException e) {
            LOG.error("new zkUtil obj error! ", e);
        }
    }
    // 查看节点
    public void ls(String node){
        try {
            System.out.println("ls / => " + zk.getChildren(node, true));
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 创建一个目录节点
    public void creatDirNode() {
        try {
            if (zk.exists("/node", true) == null) {
                zk.create("/node", "conan".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                System.out.println("create /node conan");
                // 查看/node节点数据
                System.out.println("get /node => " + new String(zk.getData("/node", false, null)));
                // 查看根节点
                System.out.println("ls / => " + zk.getChildren("/", true));
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public List<String> getTopics(String path) {
        List<String>topics = null;
        try {
            if (zk.exists(path,true) != null) {
                topics = zk.getChildren(path, true);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return topics;
    }

    public List<String> getPartitions(String topic) {
        String path = "/brokers/topics/"+topic+"/partitions";
        List<String>partitions = null;
        try {
            if (zk.exists(path,true) != null) {
                partitions = zk.getChildren(path, true);
            }
        } catch (KeeperException e) {
            LOG.error(e.getMessage(), e);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }
        return partitions;
    }

    public void setOffset(String path, long offset) {
        String mes = Long.toString(offset);
        try {
            if (zk.exists(path, true) == null) {
                String[] paths = path.split("/");
                String ps = "";
                for (String p: paths) {
                    if (!p.equals("")){
                        ps += "/"+p;
                        if (zk.exists(ps,true) == null) {
                            zk.create(ps, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        }
                    }
                }
                LOG.info("create "+path);
            }
            zk.setData(path, mes.getBytes(), -1);
        } catch (KeeperException e) {
            LOG.error(e.getMessage(),e);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(),e);
        }
    }
    public long getLastOffset(String path) {
        long offset = 0;
        try {
            if (zk.exists(path,true) != null) {
                byte[] bytes = zk.getData(path,null,null);
                offset = Long.parseLong(new String(bytes));
            }
        } catch (KeeperException e) {
            LOG.error(e.getMessage(),e);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(),e);
        }
        return offset;
    }

    public int getMongoOffset(String path) {
        int offset = 0;
        try {
            if (zk.exists(path,true) != null) {
                byte[] bytes = zk.getData(path,null,null);
                offset = Integer.parseInt(new String(bytes));
            }
        } catch (KeeperException e) {
            LOG.error(e.getMessage(),e);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(),e);
        }
        return offset;
    }
    public void setMongoOffset(String path, int offset) {
        String mes = Long.toString(offset);
        try {
            if (zk.exists(path, true) == null) {
                String[] paths = path.split("/");
                String ps = "";
                for (String p: paths) {
                    if (!p.equals("")){
                        ps += "/"+p;
                        if (zk.exists(ps,true) == null) {
                            zk.create(ps, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        }
                    }
                }
                LOG.info("create "+path);
            }
            zk.setData(path, mes.getBytes(), -1);
        } catch (KeeperException e) {
            LOG.error(e.getMessage(),e);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(),e);
        }
    }

    // 修改节点数据
    public void modify() throws KeeperException, InterruptedException {
        if (zk.exists("/node", true) != null) {
            zk.setData("/node", "changed".getBytes(), -1);
            // 查看/node节点数据
            System.out.println("get /node => " + new String(zk.getData("/node", false, null)));
        }
    }

    // 删除节点
    public void delete(String path) throws KeeperException, InterruptedException {
        if (zk.exists(path, true) != null) {
            zk.delete(path, -1);
            System.out.println("detele "+path);
        }
    }

}