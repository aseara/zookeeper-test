package com.github.aseara.zookeeper.test;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Created by qiujingde on 2017/1/17.
 * A simple example program to use com.github.aseara.zookeeper.test.DataMonitor to start and
 * stop executables based on a znode. The program watches the
 * specified znode and saves the data that corresponds to the
 * znode in the filesystem.
 */
public class Executor
    implements Watcher, Runnable, DataMonitor.DataMonitorListener {

    private DataMonitor dm;

    private Executor(String hostPort, String znode)
            throws KeeperException, IOException{
        ZooKeeper zk = new ZooKeeper(hostPort, 3000, this);
        dm = new DataMonitor(zk, znode, null, this);
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("USAGE: com.github.aseara.zookeeper.test.Executor hostPort znode");
            System.exit(2);
        }

        String hostPort = args[0];
        String znode = args[1];

        try {
            new Executor(hostPort, znode).run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /***************************************************************************
     * We do process any events ourselves, we just need to forward them on.
     *
     * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
     */
    public void process(WatchedEvent event) {
        dm.process(event);
    }

    public void run() {
        try {
            synchronized (this) {
                while (!dm.dead) {
                    wait();
                }
            }
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public void closing(int rc) {
        System.out.println("closing......");
        synchronized (this) {
            notifyAll();
        }
    }

    public void exists(byte[] data) {
        try {
            System.out.write(data == null ? new byte[]{} : data);
            System.out.println();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
