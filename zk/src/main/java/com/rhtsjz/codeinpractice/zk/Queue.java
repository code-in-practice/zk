package com.rhtsjz.codeinpractice.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by zsj on 16-5-23.
 */
public class Queue extends SyncPrimitive{
    private static final String element = "element";

    public Queue(String address, String name) {
        super(address);
        this.root = name;

        // Create ZK node name
        if(zk != null) {
            try {
                Stat stat = zk.exists(root, false);
                if (stat ==null) {
                    zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (InterruptedException e) {
                logger.warn("InterruptedException: ");
                e.printStackTrace();
            } catch (KeeperException e) {
                logger.warn("Keeper exception when instantiating queue: ");
                e.printStackTrace();
            }
        }
    }

    public boolean produce(int i) throws KeeperException, InterruptedException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byte[] value;

        // add child with value i;
        byteBuffer.putInt(i);
        value = byteBuffer.array();
        zk.create(root+"/"+element, value, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        return true;
    }

    public int consume() throws KeeperException, InterruptedException {
        int retValue = -1;
        Stat stat = null;

        // Get the first element available
        while (true) {
            synchronized (mutex) {
                List<String> list = zk.getChildren(root, true);
                if(list.size()==0) {
                    logger.info("Going to wait");
                    mutex.wait();
                }else {
                    String minStr = list.get(0).substring(element.length());
                    Integer min = new Integer(minStr);
                    for (String s: list) {
                        String tempValueStr = s.substring(element.length());
                        Integer tempValue = new Integer(tempValueStr);
                        if (tempValue < min) {
                            minStr = tempValueStr;
                            min = tempValue;
                        }
                    }
                    logger.info("Temporary value: " + root + "/" + element + minStr);
                    byte[] b = zk.getData(root+"/"+element+minStr, false, stat);
                    zk.delete(root+"/"+element+minStr,0);
                    ByteBuffer byteBuffer = ByteBuffer.wrap(b);
                    retValue = byteBuffer.getInt();
                    return retValue;
                }
            }
        }
    }

    public static void queueTest(String[] args) {
        Queue queue = new Queue(args[1], "/app1");
        logger.info("Input: " + args[1]);
        int i;
        Integer max = new Integer(args[2]);
        if(args[3].equals("p")) {
            logger.info("producer ");
            for (i=0; i<max; i++) {
                try {
                    queue.produce(10+i);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        }else {
            logger.info("consumer ");
            for (i=0; i<max; i++) {
                try {
                    int r = queue.consume();
                    logger.info("Item: {}", r);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        queueTest(args);
    }
}
