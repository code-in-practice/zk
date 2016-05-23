package com.rhtsjz.codeinpractice.zk;

/**
 * Created by zsj on 16-5-23.
 */

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;

/**
 * Barrier
 */
public class Barrier extends SyncPrimitive {
    int size;
    String name;

    public Barrier(String address, String root, int size) {
        super(address);
        this.root = root;
        this.size = size;

        // Create barrier nod
        if(zk != null) {
            try {
                Stat stat = zk.exists(root, false);
                if(stat == null) {
                    zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (InterruptedException e) {
                logger.info("InterruptedException", e);
            } catch (KeeperException e) {
                logger.info("Keeper exception when instantiating queue", e);
            }
        }

        // My node name
        try {
            name = new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
        } catch (UnknownHostException e) {
            logger.warn("UnknownHostException", e);
        }
    }

    public boolean enter() throws KeeperException, InterruptedException {
        zk.create(root + "/" + name, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        while (true) {
            synchronized (mutex) {
                List<String> list = zk.getChildren(root, true);

                if(list.size()< size){
                    mutex.wait();
                }else {
                    return true;
                }
            }
        }
    }

    public boolean leave() throws KeeperException, InterruptedException {
        zk.delete(root + "/" + name, 0);
        while (true) {
            synchronized (mutex) {
                List<String> list = zk.getChildren(root, true);
                if(list.size()>0) {
                    mutex.wait();
                }else {
                    return true;
                }
            }
        }
    }

    public static void barrierTest(String[] args) {
        Barrier barrier = new Barrier(args[1], "/b1", new Integer(args[2]));
        try {
            boolean flag = barrier.enter();
            logger.info("Entered barrier: ", args[2]);
            if(!flag) {
                logger.warn("Error when entering the barrier");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

        // Generate random integer
        Random random = new Random();
        int r = random.nextInt(100);
        // Loop for rand iterations
        for (int i=0; i<r; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        try {
            barrier.leave();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        logger.info("Left barrier");
    }

    public static void main(String[] args) {
        if(args[0].equals("qTest")) {
            //
        }else {
            for (int i=0; i<5; i++) {
                barrierTest(args);
            }
        }
    }
}
