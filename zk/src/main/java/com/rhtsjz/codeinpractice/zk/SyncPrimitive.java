package com.rhtsjz.codeinpractice.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by zsj on 16-5-23.
 */
public class SyncPrimitive implements Watcher {
    static Logger logger = LoggerFactory.getLogger(SyncPrimitive.class);
    static ZooKeeper zk = null;
    static Integer mutex;

    String root;

    SyncPrimitive(String address) {
        if(zk == null) {
            logger.info("Starting ZK: ...");
            try {
                zk = new ZooKeeper(address, 3000, this);
                mutex = new Integer(-1);
                logger.info("Finished starting ZK: {}", zk);
            } catch (IOException e) {
                logger.warn("exception", e);
                zk = null;
            }
        }else {
//            mutex = new Integer(-1);
        }
    }

    @Override
    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            logger.info("Process: {}", event.getType());
            mutex.notifyAll();
        }
    }
}
