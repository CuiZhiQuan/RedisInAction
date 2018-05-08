package com.study.redis;

import redis.clients.jedis.Jedis;

import java.util.UUID;

/**
 * @author cuizhiquan
 * @Description
 * @date 2018/5/8 23:14
 * @Copyright (c) 2017, DaChen All Rights Reserved.
 */
public class Main {

    /**
     * 第八章：构建简单的社交网站
     * @param args
     */
    public static void main(String[] args) {
        new Main().run();
    }

    public void run(){
        Jedis conn = new Jedis("localhost");
        //不select的话，默认就是0
        conn.select(0);
        //删除选中DB中所有的key
        conn.flushDB();
    }

    /**
     * 创建用户
     * @param conn
     * @param login
     * @param name
     * @return
     */
    public long createUser(Jedis conn,String login,String name){
        String lowerLogin = login.toLowerCase();
        String lock =
    }

    /**
     * 获取锁
     * @param conn
     * @param lockName
     * @param acquireTimeout
     * @param lockTimeout
     * @return
     */
    public String acquireLockWithTimeout(Jedis conn,String lockName,int acquireTimeout,int lockTimeout){
        String id = UUID.randomUUID().toString();
        lockName = "lock:" + lockName;
        long end = System.currentTimeMillis() + (acquireTimeout * 1000);
        while (System.currentTimeMillis() < end){
            if(conn.setnx(lockName,id) >= 1){

            }
        }
    }

}
