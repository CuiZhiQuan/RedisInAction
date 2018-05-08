package com.study.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.*;

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
        String lock = this.acquireLockWithTimeout(conn,"user:" + lowerLogin,10,1);
        if(Objects.isNull(lock)){
            return -1;
        }
        if(Objects.nonNull(conn.hget("users:",lowerLogin))){
            return -1;
        }
        long id = conn.incr("user:id:");
        //使用事务
        Transaction trans = conn.multi();
        trans.hset("users:",lowerLogin,String.valueOf(id));
        Map<String,String> values = new HashMap<>(16);
        values.put("login",login);
        values.put("id",String.valueOf(id));
        values.put("name",name);
        values.put("followers","0");
        values.put("following","0");
        values.put("posts","0");
        values.put("signup",String.valueOf(System.currentTimeMillis()));
        trans.hmset("user:" + id,values);
        trans.exec();
        //结束事务
        this.releaseLock(conn,"user:" + lowerLogin,lock);
        return id;
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
                conn.expire(lockName,lockTimeout);
                return id;
            }else if (conn.ttl(lockName) <= 0){
                conn.expire(lockName,lockTimeout);
            }
            try{
                Thread.sleep(1);
            }catch (InterruptedException ie){
                Thread.interrupted();
            }
        }
        return null;
    }

    /**
     * 释放锁
     * @param conn
     * @param lockName
     * @param identifier
     * @return
     */
    public boolean releaseLock(Jedis conn,String lockName,String identifier){
        lockName = "lock:" + lockName;
        while (true){
            conn.watch(lockName);
            if(identifier.equals(conn.get(lockName))){
                Transaction trans = conn.multi();
                trans.del(lockName);
                List<Object> result = trans.exec();
                // null response indicates that the transaction was aborted due
                // to the watched key changing.
                if (Objects.isNull(result)){
                    continue;
                }
                return true;
            }
            conn.unwatch();
            break;
        }
        return false;
    }
}
