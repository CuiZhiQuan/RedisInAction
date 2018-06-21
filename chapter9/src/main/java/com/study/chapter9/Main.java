package com.study.chapter9;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * @author cuizhiquan
 * @Description
 * @date 2018/6/21 9:22
 * @Copyright (c) 2017, DaChen All Rights Reserved.
 */
public class Main {

    public static void main(String[] args) {
        new Main().run();
    }

    public void run() {
        Jedis conn = new Jedis("localhost");
        conn.select(15);
        conn.flushDB();
        this.testLongZipListPerformance(conn);
    }

    /**
     * 测试长压缩列表
     * @param conn
     */
    public void testLongZipListPerformance(Jedis conn) {
        System.out.println("\n----- testLongZipListPerformance -----");
        double speed = this.longZipListPerformance(conn, "list", 1000, 1000, 100);
        System.out.println(speed);
        assert conn.llen("test") == 5;
    }


    /**
     * 长压缩列表性能测试
     * @param conn
     * @param key
     * @param length
     * @param passes
     * @param pSize
     * @return
     */
    public double longZipListPerformance(Jedis conn,String key,int length,int passes,int pSize){
        conn.del(key);
        for(int i=0;i<length;i++){
            conn.rpush(key,String.valueOf(i));
        }

        Pipeline pipeline = conn.pipelined();
        long time = System.currentTimeMillis();
        for(int p = 0 ; p < passes ; p++){
            for(int pi = 0 ; pi < pSize ; pi++){
                pipeline.rpoplpush(key,key);
            }
            pipeline.sync();
        }
        return (passes * pSize) / (System.currentTimeMillis() - time) * 1000;
    }
}
