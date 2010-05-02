/*
 * Copyright 2010 Stefan Matthias Aust. All rights reserved.
 */
package sma;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static sma.RedisClient.SortParam;

/**
 * Tests the Redis client class.
 * Requires a running Redis server without password.
 * <strong>WARNING:</strong> Database 13 gets overwritten!
 */
public class RedisClientTest extends TestCase {
  private RedisClient client;

  @Override
  protected void setUp() throws Exception {
    client = new RedisClient();
    client.selectdb(13);
  }

  @Override
  protected void tearDown() throws Exception {
    client.flushdb();
    client.close();
  }

  public void testPing() {
    assertEquals("PONG", client.ping());
  }

  public void testExists() {
    assertEquals(false, client.exists("a"));
    client.set("a", "1");
    assertEquals(true, client.exists("a"));
  }

  public void testDel() {
    assertEquals(0, client.del("a", "b"));
    client.set("a", "1");
    assertEquals(1, client.del("a", "b"));
  }

  public void testType() {
    assertEquals("none", client.type("a"));
    client.incr("b");
    assertEquals("string", client.type("b"));
    client.lpush("c", "1");
    client.rpush("c", "2");
    assertEquals("list", client.type("c"));
    client.sadd("d", "x");
    assertEquals("set", client.type("d"));
    client.zadd("e", 1.0, "x");
    assertEquals("zset", client.type("e"));
    client.hset("f", "1", "2");
    assertEquals("hash", client.type("f"));
  }

  public void testKeys() {
    client.set("a1", "1");
    client.set("a2", "1");
    client.set("b2", "1");
    client.set("c3", "1");
    Set<String> result;
    result = new HashSet<String>(Arrays.asList(client.keys("a*")));
    assertEquals(2, result.size());
    assertTrue(result.contains("a1"));
    assertTrue(result.contains("a2"));
    result = new HashSet<String>(Arrays.asList(client.keys("?2")));
    assertEquals(2, result.size());
    assertTrue(result.contains("a2"));
    assertTrue(result.contains("b2"));
  }

  public void testRandomkey() {
    assertNull(client.randomkey());
    client.incr("a");
    assertNotNull(client.randomkey());
  }

  public void testRename() {
    client.set("a", "1");
    client.set("b", "2");
    client.rename("a", "c");
    assertEquals(null, client.get("a"));
    assertEquals("1", client.get("c"));
    client.rename("c", "b");
    assertEquals(null, client.get("c"));
    assertEquals("1", client.get("b"));
  }

  public void testRenamenx() {
    client.set("a", "1");
    client.set("b", "2");
    assertFalse(client.renamenx("a", "b"));
    assertEquals("1", client.get("a"));
    assertEquals("2", client.get("b"));
    client.del("b");
    assertTrue(client.renamenx("a", "b"));
    assertEquals(null, client.get("a"));
    assertEquals("1", client.get("b"));
  }

  public void testDbSize() {
    assertEquals(0, client.dbsize());
    client.incr("a");
    client.incr("b");
    assertEquals(2, client.dbsize());
  }

  public void testExpireAndTtl() {
    assertFalse(client.expire("a", 100));
    assertEquals(-1, client.ttl("a"));
    client.incr("a");
    assertTrue(client.expire("a", 100));
    assertEquals(100, client.ttl("a"));
  }

  public void testExpireAt() {
    assertFalse(client.expireat("a", 123456789));
    client.incr("a");
    assertTrue(client.expireat("a", 123456789));
  }

  public void testMove() {
    client.selectdb(14);
    assertFalse(client.exists("___"));
    client.set("___", "!");
    client.move("___", 13);
    assertFalse(client.exists("___"));
    client.selectdb(13);
    assertEquals("!", client.get("___"));
  }

  public void testSetAndGet() {
    assertEquals(null, client.get("k1"));
    client.set("k1", "v1");
    assertEquals("v1", client.get("k1"));
    client.set("k1", "v2");
    assertEquals("v2", client.get("k1"));
  }

  public void testGetSet() {
    client.set("k1", "v1");
    assertEquals("v1", client.getset("k1", "v2"));
    assertEquals("v2", client.get("k1"));
  }

  public void testMget() {
    client.set("k1", "1");
    client.set("k2", "2");
    assertEquals(strings("2", "1"), client.mget("k2", "k1"));
  }

  public void testSetnx() {
    assertTrue(client.setnx("k1", "v1"));
    assertFalse(client.setnx("k1", "v2"));
    assertEquals("v1", client.get("k1"));
  }

  public void testSetex() {
    client.setex("k1", "v", 100);
    assertEquals(100, client.ttl("k1"));
    assertEquals("v", client.get("k1"));
  }

  public void testMset() {
    client.mset("k1", "v1", "k2", "v2");
    assertEquals("v1", client.get("k1"));
    assertEquals("v2", client.get("k2"));
  }

  public void testMsetnx() {
    assertTrue(client.msetnx("k1", "v1", "k2", "v2"));
    assertEquals("v1", client.get("k1"));
    assertEquals("v2", client.get("k2"));
    assertFalse(client.msetnx("k2", "v3", "k3", "v4"));
    assertEquals("v2", client.get("k2"));
    assertEquals(null, client.get("k3"));
  }

  public void testIncr() {
    assertEquals(1, client.incr("a"));
    assertEquals(2, client.incr("a"));
    assertEquals(4, client.incr("a", 2));
    client.set("b", "43");
    assertEquals(42, client.incr("b", -1));
  }

  public void testDecr() {
    assertEquals(-1, client.decr("a"));
    assertEquals(-2, client.decr("a"));
    assertEquals(-4, client.decr("a", 2));
    client.set("b", "41");
    assertEquals(42, client.decr("b", -1));
  }

  public void testAppend() {
    assertEquals(3, client.append("a", "xyz"));
    assertEquals(5, client.append("a", ".."));
  }

  public void testSubstr() {
    assertEquals(null, client.substr("a", -2, -1));
    client.set("a", "hello");
    assertEquals("lo", client.substr("a", -2, -1));
  }

  public void testLpushAndRpushAndLlen() {
    assertEquals(0, client.llen("x"));
    client.lpush("x", "1");
    client.rpush("x", "2");
    assertEquals(2, client.llen("x"));
  }

  public void testRrange() {
    assertEquals(strings(), client.lrange("x", 0, -1));
    client.lpush("x", "1");
    client.rpush("x", "2");
    assertEquals(strings("1", "2"), client.lrange("x", 0, -1));
    assertEquals(strings("2"), client.lrange("x", -1, -1));
  }

  public void testLTrim() {
    client.lpush("x", "1");
    client.rpush("x", "2");
    client.rpush("x", "3");
    client.ltrim("x", 0, 1);
    assertEquals(strings("1", "2"), client.lrange("x", 0, -1));
    client.ltrim("x", 1, 1);
    assertEquals(strings("2"), client.lrange("x", 0, -1));
  }

  public void testLindex() {
    assertEquals(null, client.lindex("x", 0));
    client.lpush("x", "1");
    client.rpush("x", "2");
    client.rpush("x", "3");
    assertEquals("1", client.lindex("x", 0));
    assertEquals("2", client.lindex("x", 1));
    assertEquals("3", client.lindex("x", -1));
  }

  public void testLset() {
    client.lpush("x", "1");
    client.lpush("x", "2");
    client.lset("x", 0, "3");
    assertEquals("3", client.lindex("x", 0));
  }

  public void testLrem() {
    assertEquals(0, client.lrem("x", 2, "1"));
    client.lpush("x", "1");
    client.lpush("x", "2");
    client.lpush("x", "1");
    client.lpush("x", "1");
    client.lpush("x", "3");
    assertEquals(2, client.lrem("x", 2, "1"));
    assertEquals(1, client.lrem("x", 2, "1"));
    assertEquals(0, client.lrem("x", 2, "1"));
    assertEquals(2, client.llen("x"));
  }

  public void testLpop() {
    assertEquals(null, client.lpop("x"));
    client.lpush("x", "a");
    assertEquals("a", client.lpop("x"));
    assertEquals(0, client.llen("x"));
  }

  public void testRpop() {
    assertEquals(null, client.rpop("x"));
    client.rpush("x", "a");
    assertEquals("a", client.rpop("x"));
    assertEquals(0, client.llen("x"));
  }

  public void testblpop() {
    client.lpush("x", "1");
    client.lpush("x", "2");
    client.lpush("z", "3");
    assertEquals(strings("x", "2"), client.blpop("x", "y", "z"));
  }

  public void testbrpop() {
    client.rpush("x", "1");
    client.rpush("x", "2");
    client.rpush("z", "2");
    assertEquals(strings("x", "1"), client.brpop("x", "y", "z"));
  }

  public void testBlpopAndBrpopTimeout() {
    //assertNull(client.blpop(1, "y"));
    //assertNull(client.brpop(1, "y"));
  }

  public void testRpoplpush() {
    assertEquals(null, client.rpoplpush("x", "y"));

    client.lpush("x", "1");
    assertEquals("1", client.rpoplpush("x", "y"));
    assertEquals(0, client.llen("x"));
    assertEquals(1, client.llen("y"));

    client.lpush("z", "2");
    assertEquals("1", client.rpoplpush("y", "z"));
    assertEquals(0, client.llen("y"));
    assertEquals(2, client.llen("z"));
  }

  public void testSaddAndSrem() {
    assertTrue(client.sadd("k", "a"));
    assertFalse(client.sadd("k", "a"));
    assertTrue(client.srem("k", "a"));
    assertFalse(client.srem("k", "a"));
    assertFalse(client.srem("_", "a"));
  }

  public void testSpop() {
    client.sadd("k", "a");
    assertEquals("a", client.spop("k"));
    assertNull(client.spop("k"));
    assertNull(client.spop("_"));
  }

  public void testSmove() {
    client.sadd("k", "a");
    assertFalse(client.smove("_", "k", "a"));
    assertFalse(client.smove("k", "_", "_"));
    assertTrue(client.smove("k", "_", "a"));
  }

  public void testScard() {
    assertEquals(0, client.scard("k"));
    client.sadd("k", "1");
    client.sadd("k", "2");
    assertEquals(2, client.scard("k"));
  }

  public void testSismember() {
    client.sadd("k", "1");
    assertFalse(client.sismember("_", "1"));
    assertFalse(client.sismember("k", "_"));
    assertTrue(client.sismember("k", "1"));
  }

  public void testSinter() {
    assertEquals(strings(), client.sinter("k1"));
    assertEquals(strings(), client.sinter("k1", "k2"));
    client.sadd("k1", "1");
    client.sadd("k1", "2");
    client.sadd("k2", "2");
    client.sadd("k2", "3");
    assertEquals(set("2"), set(client.sinter("k1", "k2")));
    client.sinterstore("k3", "k1", "k2");
    assertEquals(set("2"), set(client.smembers("k3")));
  }

  public void testSunion() {
    assertEquals(strings(), client.sunion("k1"));
    assertEquals(strings(), client.sunion("k1", "k2"));
    client.sadd("k1", "1");
    client.sadd("k1", "2");
    client.sadd("k2", "2");
    client.sadd("k2", "3");
    assertEquals(set("1", "2", "3"), set(client.sunion("k1", "k2")));
    client.sunionstore("k3", "k1", "k2");
    assertEquals(set("1", "2", "3"), set(client.smembers("k3")));
  }

  public void testSdiff() {
    assertEquals(strings(), client.sdiff("k1"));
    assertEquals(strings(), client.sdiff("k1", "k2"));
    client.sadd("k1", "1");
    client.sadd("k1", "2");
    client.sadd("k2", "2");
    client.sadd("k2", "3");
    assertEquals(set("1"), set(client.sdiff("k1", "k2")));
    client.sdiffstore("k3", "k1", "k2");
    assertEquals(set("1"), set(client.smembers("k3")));
  }

  public void testSmembers() {
    client.sadd("k", "1");
    client.sadd("k", "2");
    assertEquals(set(), set(client.smembers("_")));
    assertEquals(set("1", "2"), set(client.smembers("k")));
  }

  public void testSrandmember() {
    client.sadd("k", "1");
    assertNull(client.srandmember("_"));
    assertNotNull(client.srandmember("k"));
  }

  public void testZadd() {
    assertTrue(client.zadd("a", 1.0, "x"));
    assertFalse(client.zadd("a", 0.5, "x"));
    assertTrue(client.zadd("a", 2.0, "y"));
    assertEquals(strings("x", "y"), client.zrange("a", 0, -1));
  }

  public void testZrem() {
    assertFalse(client.zrem("a", "x"));
    client.zadd("a", 5, "x");
    assertTrue(client.zrem("a", "x"));
  }

  public void testZincrby() {
    assertEquals(3.3, client.zincrby("b", 3.3, "x"));
    assertEquals(4,0, client.zincrby("b", 0.7, "x"));
    assertEquals(1.5, client.zincrby("b", -2.5, "x"));
  }

  public void testZrankAndZrevrank() {
    client.zadd("a", 1.2, "x");
    client.zadd("a", 0.4, "y");
    assertEquals(0, client.zrank("a", "y"));
    assertEquals(1, client.zrank("a", "x"));
    assertEquals(-1, client.zrank("a", "z"));
    assertEquals(1, client.zrevrank("a", "y"));
    assertEquals(0, client.zrevrank("a", "x"));
    assertEquals(-1, client.zrevrank("a", "z"));
  }

  public void testZrangeAndZrevrange() {
    assertEquals(strings(), client.zrange("a", 0, -1));
    assertEquals(strings(), client.zrevrange("a", 0, -1));
    client.zadd("a", 1.2, "x");
    client.zadd("a", 0.4, "y");
    assertEquals(strings("y", "x"), client.zrange("a", 0, -1));
    assertEquals(strings("x", "y"), client.zrevrange("a", 0, -1));
  }

  public void testZrangebyscore() {
    assertEquals(strings(), client.zrangebyscore("a", 0.0, 9.9));
    client.zadd("a", 0.0, "x");
    client.zadd("a", 0.1, "y");
    client.zadd("a", 0.2, "z");
    assertEquals(strings("y", "z"), client.zrangebyscore("a", 0.1, 9.9));
    assertEquals(strings("z"), client.zrangebyscore("a", 0.1, 9.9, 1, 99));
  }

  public void testZremrangebyrank() {
    assertEquals(0, client.zremrangebyrank("a", 0, 1));
    client.zadd("a", 0.0, "x");
    client.zadd("a", 0.1, "y");
    client.zadd("a", 0.2, "z");
    assertEquals(2, client.zremrangebyrank("a", 0, 1));
    assertEquals(1, client.zcard("a"));
  }

  public void testZremrangebyscore() {
    assertEquals(0, client.zremrangebyscore("a", 0.1, 0.9));
    client.zadd("a", 0.0, "x");
    client.zadd("a", 0.1, "y");
    client.zadd("a", 0.2, "z");
    assertEquals(2, client.zremrangebyscore("a", 0.1, 0.9));
    assertEquals(1, client.zcard("a"));
  }

  public void testZcard() {
    assertEquals(0, client.zcard("c"));
    client.zincrby("c", 1.2, "x");
    client.zincrby("c", 2.4, "y");
    assertEquals(2, client.zcard("c"));
  }

  public void testZscore() {
    assertEquals(null, client.zscore("c", "a"));
    client.zadd("c", 0.0, "b");
    assertEquals(null, client.zscore("c", "a"));
    assertEquals(0.0, client.zscore("c", "b"));
  }

  public void testZunion() {
    client.zadd("a", 1.1, "X");
    client.zadd("b", 2.2, "Y");
    assertEquals(2, client.zunion("c", new String[]{"a", "b"}, null, null));
    assertEquals(strings("X", "Y"), client.zrange("c", 0, -1));

    assertEquals(2, client.zunion("d", new String[]{"a", "b"}, new double[]{1.5, 0.5}, null));
    assertEquals(strings("Y", "X"), client.zrange("d", 0, -1));
    assertEquals(1.1, client.zscore("d", "Y"), 0.001);
    assertEquals(1.65, client.zscore("d", "X"), 0.001);

    client.zadd("b", 3.0, "X");
    client.zunion("c", new String[]{"a", "b"}, null, RedisClient.Aggregate.MIN);
    assertEquals(1.1, client.zscore("c", "X"));
    client.zunion("c", new String[]{"a", "b"}, null, RedisClient.Aggregate.MAX);
    assertEquals(3.0, client.zscore("c", "X"));
  }

  public void testZinter() {
    client.zadd("a", 1.1, "X");
    client.zadd("b", 0.5, "X");
    client.zadd("b", 2.2, "Y");
    assertEquals(1, client.zinter("c", new String[]{"a", "b"}, null, null));
    assertEquals(strings("X"), client.zrange("c", 0, -1));
    assertEquals(1.6, client.zscore("c", "X"), 0.001);

    assertEquals(1, client.zinter("d", new String[]{"a", "b"}, new double[]{1.5, 0.5}, null));
    assertEquals(strings("X"), client.zrange("d", 0, -1));
    assertEquals(1.9, client.zscore("d", "X"), 0.001);
  }

  public void testHgetAndHset() {
    assertTrue(client.hset("k", "a", "abc"));
    assertFalse(client.hset("k", "a", "abc"));
    assertEquals("abc", client.hget("k", "a"));
    assertEquals(null, client.hget("k", "_"));
    assertEquals(null, client.hget("_", "a"));
  }

  public void testHsetnx() {
    assertTrue(client.hsetnx("k", "a", "abc"));
    assertFalse(client.hsetnx("k", "a", "def"));
    assertEquals("abc", client.hget("k", "a"));
  }

  public void testHmset() {
    client.hmset("k", "a", "1", "b", "2");
    assertEquals("1", client.hget("k", "a"));
    assertEquals("2", client.hget("k", "b"));
  }

  public void testHmsetInvalid() {
    try {
      client.mset();
      fail();
    } catch (IllegalArgumentException e) {
    }
    try {
      client.mset("k");
      fail();
    } catch (IllegalArgumentException e) {
    }
    try {
      client.mset("k", "v", "k");
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  public void testHincrAndHdecr() {
    assertEquals(1, client.hincr("k", "a"));
    assertEquals(2, client.hincr("k", "a"));
    assertEquals(4, client.hincrby("k", "a", 2));
    assertEquals(-1, client.hdecr("k", "b"));
    assertEquals(-2, client.hdecr("k", "b"));
    assertEquals(-4, client.hdecrby("k", "b", 2));
  }

  public void testHexistsAndHdel() {
    assertFalse(client.hexists("k", "a"));
    client.hset("k", "a", "1");
    assertTrue(client.hexists("k", "a"));
    assertTrue(client.hdel("k", "a"));
    assertFalse(client.hdel("k", "a"));
    assertFalse(client.hexists("k", "a"));
  }

  public void testHlen() {
    assertEquals(0, client.hlen("k"));
    client.hset("k", "a", "1");
    client.hset("k", "b", "2");
    assertEquals(2, client.hlen("k"));
  }

  public void testHkeysAndHvals() {
    assertEquals(strings(), client.hkeys("k"));
    assertEquals(strings(), client.hvals("k"));
    client.hset("k", "a", "1");
    client.hset("k", "b", "2");
    assertEquals(strings("a", "b"), client.hkeys("k"));
    assertEquals(strings("1", "2"), client.hvals("k"));
  }

  public void testHgetall() {
    assertEquals(strings(), client.hgetall("k"));
    client.hset("k", "a", "1");
    client.hset("k", "b", "2");
    assertEquals(strings("a", "1", "b", "2"), client.hgetall("k"));
  }

  public void testSortList() {
    client.lpush("k", "2");
    client.lpush("k", "1");
    client.lpush("k", "11");
    client.lpush("k", "X");
    sorting();
  }

  public void testSortSet() {
    client.sadd("k", "2");
    client.sadd("k", "1");
    client.sadd("k", "11");
    client.sadd("k", "X");
    sorting();
  }

  private void sorting() {
    client.set("foo_1", "A");
    client.set("foo_2", "B");
    client.set("w_1", "0.3");
    client.set("w_2", "0.1");
    assertEquals(strings(), client.sort("_", null, -1, -1, null, false, false));
    assertEquals(strings("X", "1", "2", "11"), client.sort("k", null, -1, -1, null, false, false));
    assertEquals(strings("11", "2", "1", "X"), client.sort("k", null, -1, -1, null, true, false));
    assertEquals(strings("1", "2"), client.sort("k", null, 1, 2, null, false, false));
    assertEquals(strings("1", "11", "2", "X"), client.sort("k", null, -1, -1, null, false, true));
    assertEquals(4, client.sortstore("k", null, -1, -1, null, true, true, "kk"));
    assertEquals(strings("X", "2", "11", "1"), client.lrange("kk", 0, -1));
    assertEquals(strings("A", "1", "B", "2", null, "11"),
        client.sort("k", null, 1, 3, new String[]{"foo_*", "#"}, false, false));
    assertEquals(strings("A", "1", "B", "2", null, "11"),
        client.sort("k", SortParam.limit(1, 3), SortParam.get("foo_*"), SortParam.get("#")));
    assertEquals(strings("11", "2", "1"), client.sort("k", "w_*", 1, 3, null, false, false));
    assertEquals(strings("11", "2", "1"), client.sort("k", SortParam.by("w_*"), SortParam.limit(1, 3)));
  }

  public void testMultiAndExec() {
    client.multi();
    client.incr("a");
    client.incr("a", 3);
    assertEquals(new Object[]{1, 4}, client.exec());
  }

  public void testMultiAndDiscard() {
    client.multi();
    client.incr("a");
    client.incr("a", 3);
    client.discard();
    assertFalse(client.exists("a"));
  }

  // TODO pub/sub tests (missing implementation)

  public void testSave() {
    client.save();
    client.bgsave();
    client.bgrewriteaof();
    assertTrue(client.lastsave() != 0);
  }

  public void testInfo() {
    assertTrue(client.info().length() != 0);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  private static void assertEquals(Object[] expected, Object[] actual) {
    if (!Arrays.equals(expected, actual)) {
      throw new AssertionFailedError(
          "expected:<" + Arrays.toString(expected) + "> but was:<" + Arrays.toString(actual) + ">");
    }
  }

  private static String[] strings(String... strings) {
    return strings;
  }

  private static Set<String> set(String... strings) {
    return new HashSet<String>(Arrays.asList(strings));
  }
}
