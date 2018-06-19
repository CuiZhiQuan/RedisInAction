package com.study.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.util.*;

/**
 * @author cuizhiquan
 * @Description 第八章：构建简单的社交网站
 * @date 2018/5/8 23:14
 * @Copyright (c) 2017, DaChen All Rights Reserved.
 */
public class Main {

    private static int HOME_TIMELINE_SIZE = 1000;

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

    /**
     * 创建状态消息
     * @param conn
     * @param uid
     * @param message
     * @return
     */
    public long createStatus(Jedis conn,long uid,String message){
        return createStatus(conn, uid, message,null);
    }

    /**
     * 创建状态消息
     * @param conn
     * @param uid
     * @param message
     * @param data
     * @return
     */
    public long createStatus(Jedis conn,long uid,String message,Map<String,String> data){
        Transaction trans = conn.multi();
        trans.hget("user:" + uid,"login");
        trans.incr("status:id:");
        List<Object> response = trans.exec();
        String login = (String)response.get(0);
        long id = (Long)response.get(1);
        if(Objects.isNull(login)){
            return -1;
        }
        if(Objects.isNull(data)){
            data = new HashMap<>(2);
        }
        data.put("message",message);
        data.put("posted",String.valueOf(System.currentTimeMillis()));
        data.put("id",String.valueOf(id));
        data.put("uid",String.valueOf(uid));
        data.put("login",login);

        trans = conn.multi();
        trans.hmset("status:" + id,data);
        trans.hincrBy("user:" + uid,"posts",1);
        trans.exec();
        return id;
    }

    /**
     * 根据时间线获取状态消息
     * @param conn
     * @param uid
     * @return
     */
    public List<Map<String,String>> getStatusMessage(Jedis conn,long uid){
        return getStatusMessage(conn, uid,0,30);
    }

    /**
     * 根据时间线获取状态消息
     * @param conn
     * @param uid
     * @param page
     * @param count
     * @return
     */
    public List<Map<String,String>> getStatusMessage(Jedis conn,long uid,int page,int count){
        Set<String> statusIds = conn.zrevrange("home:" + uid, page * count, page * count - 1);
        Transaction trans = conn.multi();
        for(String id : statusIds){
            trans.hgetAll("status:" + id);
        }
        List<Map<String,String>> statuses = new LinkedList<>();
        for(Object result : trans.exec()){
            Map<String,String> status = (Map<String, String>)result;
            if(Objects.nonNull(status) && status.size() > 0){
                statuses.add(status);
            }
        }
        return statuses;
    }

    /**
     * 关注某个用户
     * @param conn
     * @param uid
     * @param otherUid
     * @return
     */
    public boolean followUser(Jedis conn,long uid,long otherUid){
        String followingKey = "following:" + uid;
        String followerKey = "followers:" + otherUid;
        Double score = conn.zscore(followingKey,String.valueOf(otherUid));
        if(Objects.nonNull(score)){
            return false;
        }
        long now = System.currentTimeMillis();
        Transaction trans = conn.multi();
        trans.zadd(followingKey,now,String.valueOf(otherUid));
        trans.zadd(followerKey, now, String.valueOf(uid));
        trans.zcard(followingKey);
        trans.zcard(followerKey);
        trans.zrevrangeWithScores("profile:" + otherUid,0,HOME_TIMELINE_SIZE-1);

        List<Object> response = trans.exec();
        long following = (Long)response.get(response.size() - 3);
        long followers = (Long) response.get(response.size() - 2);
        Set<Tuple> statuses = (Set<Tuple>)response.get(response.size() - 1);

        trans = conn.multi();
        trans.hset("user:" + uid, "following", String.valueOf(following));
        trans.hset("user:" + otherUid, "followers", String.valueOf(followers));

        if (statuses.size() > 0) {
            for (Tuple status : statuses){
                trans.zadd("home:" + uid, status.getScore(), status.getElement());
            }
        }
        trans.zremrangeByRank("home:" + uid, 0, 0 - HOME_TIMELINE_SIZE - 1);
        trans.exec();
        return false;
    }

    /**
     * 取消关注某个用户
     * @param conn
     * @param uid
     * @param otherUid
     * @return
     */
    public boolean unfollowUser(Jedis conn,long uid,long otherUid){
        String followingKey = "following:" + uid;
        String followerKey = "follower:" + otherUid;

        Double score = conn.zscore(followingKey,String.valueOf(otherUid));
        if(Objects.isNull(score)){
            return false;
        }

        Transaction trans = conn.multi();
        trans.zrem(followingKey,String.valueOf(otherUid));
        trans.zrem(followerKey,String.valueOf(uid));
        trans.zcard(followingKey);
        trans.zcard(followerKey);
        trans.zrevrange("profile:" + otherUid,0,HOME_TIMELINE_SIZE -1);
        List<Object> response = trans.exec();
        long following = (Long)response.get(response.size() - 3);
        long follower = (Long)response.get(response.size() - 2);
        Set<String> statuses = (Set<String>)response.get(response.size() - 1);

        trans = conn.multi();
        trans.hset("user:" + uid,"following",String.valueOf(following));
        trans.hset("user:" + otherUid,"follower",String.valueOf(follower));
        if(statuses.size() > 0){
            for(String status : statuses){
                trans.zrem("home:" + uid,status);
            }
        }
        trans.exec();
        return true;
    }
}
