/*
 * Copyright 2010 Stefan Matthias Aust. All rights reserved.
 */
package sma;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Implements a simple thread-safe Redis 1.3.10 client.
 */
public class RedisClient {
  public static final int DEFAULT_PORT = 6379;

  private final String host;
  private final int port;

  private static class Handler {
    private final Socket socket;
    private final BufferedInputStream in;
    private final BufferedOutputStream out;

    /**
     * Constructs a new socket connection to the Redis server, setting up buffered input and output streams.
     */
    Handler(String host, int port) {
      try {
        socket = new Socket(host, port);
        in = new BufferedInputStream(socket.getInputStream());
        out = new BufferedOutputStream(socket.getOutputStream());
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    }

    /**
     * Closes the socket connection.
     */
    void close() {
      try {
        socket.close();
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    }

    /**
     * Sends a simple command string (which is conveniently encoded as UTF-8) and returns the answer.
     */
    Object sendInline(String cmd) {
      try {
        out.write(bytes(cmd));
        out.write('\r');
        out.write('\n');
        out.flush();
        return answer();
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    }

    /**
     * Sends a command string (which is conveniently encoded as UTF-8) with one argument as bulk command.
     */
    Object sendBulk(String cmd, String data) {
      return sendBulk(cmd, bytes(data));
    }

    /**
     * Sends a command string (which is conveniently encoded as UTF-8) with one argument as bulk command.
     */
    Object sendBulk(String cmd, byte[] data) {
      try {
        out.write(bytes(cmd));
        write(' ', data);
        out.flush();
        return answer();
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    }

    Object sendMultiBulk(String cmd, byte[][] datas) {
      try {
        out.write('*');
        out.write(bytes(Integer.toString(datas.length + 1)));
        out.write('\r');
        out.write('\n');
        write('$', bytes(cmd));
        for (int i = 0; i < datas.length; i++) {
          write('$', datas[i]);
        }
        out.flush();
        return answer();
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    }

    private void write(int prefix, byte[] data) throws IOException {
      out.write(prefix);
      out.write(bytes(Integer.toString(data.length)));
      out.write('\r');
      out.write('\n');
      out.write(data);
      out.write('\r');
      out.write('\n');
    }

    /**
     * Awaits and returns a answer. The answer is either an inline string, a bulk data byte[],
     * an array of data byte[]s, an integer or an Redis exception is raised.
     */
    private Object answer() throws IOException {
      String answer = readLine();
      if (answer.length() == 0) {
        throw new RedisException("missing answer");
      }
      switch (answer.charAt(0)) {
        case '-':
          throw new RedisException(answer.substring(1));
        case '+':
          return answer.substring(1);
        case '$': {
          return readFully(Integer.parseInt(answer.substring(1)));
        }
        case '*': {
          int len = Integer.parseInt(answer.substring(1));
          if (len < 0) {
            return null;
          }
          byte[][] datas = new byte[len][];
          for (int i = 0; i < len; i++) {
            datas[i] = readFully(Integer.parseInt(readLine().substring(1)));
          }
          return datas;
        }
        case ':':
          return new Integer(answer.substring(1));
        default:
          throw new RedisException("invalid answer: " + answer);
      }
    }

    /**
     * Reads and returns one line terminated with \r\n as String (without \r\n).
     */
    private String readLine() throws IOException {
      ByteArrayOutputStream b = new ByteArrayOutputStream(256);
      int ch = in.read();
      while (ch != -1) {
        if (ch == '\r') {
          ch = in.read();
          if (ch == '\n') {
            break;
          }
          b.write('\r');
        }
        b.write(ch);
        ch = in.read();
      }
      return b.toString("utf-8");
    }

    /**
     * Reads and returns a <code>byte[]</code> with the given number of bytes
     * or returns <code>null</code> if the number of bytes is negative.
     */
    private byte[] readFully(int len) throws IOException {
      if (len < 0) {
        return null;
      }
      byte[] data = new byte[len];
      int off = 0;
      while (len > 0) {
        int read = in.read(data, off, len);
        off += read;
        len -= read;
      }
      // skip final CR LF
      in.read();
      in.read();
      return data;
    }
  }

  /**
   * Stores all handlers opened in all threads so that {@link #close()} can close them all.
   */
  private final List<Handler> handlers = new ArrayList<Handler>();

  /**
   * Stores one handler per thread to make the client thread-safe.
   */
  private final ThreadLocal<Handler> handler = new ThreadLocal<Handler>() {

    @Override
    protected Handler initialValue() {
      Handler handler = new Handler(host, port);
      synchronized (handlers) {
        handlers.add(handler);
      }
      return handler;
    }
  };


  /**
   * Constructs a new client for <code>localhost</code> and default port <code>6379</code>.
   * This client is thread-safe in that it will automatically open a new connection per thread.
   */
  public RedisClient() {
    this("localhost", DEFAULT_PORT);
  }

  /**
   * Constructs a new client for the given host and default port <code>6379</code>.
   * This client is thread-safe in that it will automatically open a new connection per thread.
   * @param host name of the host where a Redis server is running
   */
  public RedisClient(String host) {
    this(host, DEFAULT_PORT);
  }

  /**
   * Constructs a new client for the given host and port.
   * This client is thread-safe in that it will automatically open a new connection per thread.
   * @param host name of the host where a Redis server is running
   * @param port port number the Redis server is listening on
   */
  public RedisClient(String host, int port) {
    this.host = host;
    this.port = port;
  }

  /**
   * Closes the server connection.
   */
  public void close() {
    synchronized (handlers) {
      for (Handler handler : handlers) {
        handler.close();
      }
    }
  }

  /**
   * Requests for authentication in a password protected Redis server.
   * Must be the first command if a server is password protected. No-op on a password free server.
   * @param password the password configured
   * @throws RedisException if the password is wrong
   */
  public void auth(String password) {
    sendInline("AUTH " + password);
  }

  /**
   * Returns <code>PONG</code> from the server to test whether its still alive and kicking. 
   */
  public String ping() {
    return (String) sendInline("PING");
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Test if the specified key exists.
   * The command returns <code>true</code> if the key exists, otherwise <code>false</code> is returned.
   * Note that even keys set with an empty string as value will return <code>true</code>.
   */
  public boolean exists(String key) {
    return bool(sendInline("EXISTS " + key));
  }

  /**
   * Remove the specified keys.
   * If a given key does not exist no operation is performed for this key.
   * The commnad returns the number of keys removed.
   */
  public int del(String... keys) {
    checkNotEmpty(keys);
    return integer(sendInline("DEL " + join(keys, " ")));
  }

  /**
   * Return the type of the value stored at key in form of a string.
   * The type can be one of "none", "string", "list", "set", "zset", or "hash.
   * "none" is returned if the key does not exist.
   */
  public String type(String key) {
    return (String) sendInline("TYPE " + key);
  }

  /**
   * Returns all the keys matching the glob-style pattern as space separated strings.
   * For example if you have in the database the keys "foo" and "foobar" the command <code>KEYS foo*</code> will return
   * "foo foobar".
   *
   * Note that while the time complexity for this operation is O(n) the constant times are pretty low.
   * For example Redis running on an entry level laptop can scan a 1 million keys database in 40 milliseconds.
   * Still it's better to consider this one of the slow commands that may ruin the DB performance if not used with care.
   * In other words this command is intended only for debugging and special operations like creating a script to change
   * the DB schema. Don't use it in your normal code. Use Redis Sets in order to group together a subset of objects.
   *
   * Glob style patterns examples:
   * <ul>
   * <li><code>h?llo</code> will match hello hallo hhllo
   * <li><code>h*llo</code> will match hllo heeeello
   * <li><code>h[ae]llo</code> will match hello and hallo, but not hillo
   * </ul>
   * Use <code>\</code> to escape special chars if you want to match them verbatim.
   */
  public String[] keys(String pattern) {
    return strings(sendInline("KEYS " + pattern));
  }

  /**
   * Return a randomly selected key from the currently selected DB.
   */
  public String randomkey() {
    return string(sendInline("RANDOMKEY"));
  }

  /**
   * Atomically renames the key oldkey to newkey.
   * If the source and destination name are the same an error is thrown.
   * If newkey already exists it is overwritten.
   */
  public void rename(String oldkey, String newkey) {
    sendInline("RENAME " + oldkey + " " + newkey);
  }

  /**
   * Rename oldkey into newkey but fails if the destination key newkey already exists.
   * Return <code>true</code> if the key was renamed and <code>false</code> otherwise.
   */
  public boolean renamenx(String oldkey, String newkey) {
    return bool(sendInline("RENAMENX " + oldkey + " " + newkey));
  }

  /**
   * Return the number of keys in the currently selected database.
   */
  public int dbsize() {
    return integer(sendInline("DBSIZE"));
  }

  /**
   * Set a timeout on the specified key.
   * After the timeout the key will be automatically delete by the server.
   * A key with an associated timeout is said to be volatile in Redis terminology.
   * Return <code>true</code> if the timeout was set and <code>false</code> otherwise.
   */
  public boolean expire(String key, int seconds) {
    return bool(sendInline("EXPIRE " + key + " " + seconds));
  }

  /**
   * Set a timeout on the specified key.
   * After the timeout the key will be automatically delete by the server.
   * A key with an associated timeout is said to be volatile in Redis terminology.
   * Return <code>true</code> if the timeout was set and <code>false</code> otherwise.
   */
  public boolean expireat(String key, int unixtime) {
    return bool(sendInline("EXPIREAT " + key + " " + unixtime));
  }

  /**
   * The TTL command returns the remaining time to live in seconds of a key that has an EXPIRE set.
   * This introspection capability allows a Redis client to check how many seconds a given key will continue to be part
   * of the dataset. If the key does not exists or does not have an associated expire, -1 is returned.
   */
  public int ttl(String key) {
    return integer(sendInline("TTL " + key));
  }

  /**
   * Select the DB with having the specified zero-based numeric index.
   * For default every new client connection is automatically selectedto DB 0.
   * @param index by default 0..15 is allowed
   */
  public void selectdb(int index) {
    sendInline("SELECT " + index);
  }

  /**
   * Move the specified key from the currently selected DB to the specified destination DB.
   * Note that this command returns <code>true</code> only if the key was successfully moved, and <code>false</code>
   * if the target key was already there or if the source key was not found at all, so it is possible to use MOVE as
   * a locking primitive.
   */
  public boolean move(String key, int dbindex) {
    return bool(sendInline("MOVE " + key + " " + dbindex));
  }

  /**
   * Delete all the keys of the currently selected DB. This command never fails.
   */
  public void flushdb() {
    sendInline("FLUSHDB");
  }

  /**
   * Delete all the keys of all the existing databases, not just the currently selected one.
   * This command never fails.
   */
  public void flushall() {
    sendInline("FLUSHALL");
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Get the value of the specified key (String command).
   * If the key does not exist, <code>null</code> is returned.
   * If the value stored at key is not a string an error is thrown.
   */
  public String get(String key) {
    return string(sendInline("GET " + key));
  }

  /**
   * Set the string value as value of the key (String command).
   * The string can't be longer than 1073741824 bytes (1 GB).
   */
  public void set(String key, String value) {
    sendBulk("SET " + key, value);
  }

  /**
   * GETSET is an atomic set this value and return the old value command (String command).
   * Set key to the string value and return the old value stored at key.
   * The string can't be longer than 1073741824 bytes (1 GB).
   */
  public String getset(String key, String value) {
    return string(sendBulk("GETSET " + key, value));
  }

  /**
   * Get the values of all the specified keys (String command).
   * If one or more keys don't exist or is not of type String, <code>null</code> is returned instead of the value
   * of the specified key, but the operation never fails.
   */
  public String[] mget(String... keys) {
    checkNotEmpty(keys);
    return strings(sendInline("MGET " + join(keys, " ")));
  }

  /**
   * SETNX works exactly like SET with the only difference that if the key already exists no operation is performed.
   * SETNX actually means "SET if Not eXists".
   * Return <code>true</code> if the key was set and <code>false</code> otherwise.
   */
  public boolean setnx(String key, String value) {
    return bool(sendBulk("SETNX " + key, value));
  }

  /**
   * SETEX is exactly equivalent to the following group of commands:
   * <pre>
   * SET <i>key</i> <i>value</i>
   * EXPIRE <i>key</i> <i>time</i>
   * </pre>
   * The operation is atomic. An atomic SET+EXPIRE operation was already provided using MULTI/EXEC, but SETEX
   * is a faster alternative provided because this operation is very common when Redis is used as a Cache.
   */
  public void setex(String key, String value, int seconds) {
    sendBulk("SETEX " + key + " " + seconds, value);
  }

  public void mset(String... keysAndValues) {
    if (keysAndValues.length < 2 || (keysAndValues.length % 2) == 1) {
      throw new IllegalArgumentException();
    }
    sendMultiBulk("MSET", bytes(keysAndValues));
  }

  public boolean msetnx(String... keysAndValues) {
    if (keysAndValues.length < 2 || (keysAndValues.length % 2) == 1) {
      throw new IllegalArgumentException();
    }
    return bool(sendMultiBulk("MSETNX", bytes(keysAndValues)));
  }

  public int incr(String key) {
    return integer(sendInline("INCR " + key));
  }

  public int incr(String key, int offset) {
    if (offset == 1) {
      return incr(key);
    }
    return integer(sendInline("INCRBY " + key + " " + offset));
  }

  public int decr(String key) {
    return integer(sendInline("DECR " + key));
  }

  public int decr(String key, int offset) {
    if (offset == 1) {
      return decr(key);
    }
    return integer(sendInline("DECRBY " + key + " " + offset));
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Add the string value to the tail of the list stored at key (List command).
   * If the key does not exist an empty list is created just before the append operation.
   * If the key exists but is not a List an error is raised.
   */
  public void rpush(String key, String value) {
    sendBulk("RPUSH " + key, value);
  }

  /**
   * Add the string value to the head of the list stored at key (List command).
   * If the key does not exist an empty list is created just before the append operation.
   * If the key exists but is not a List an error is raised.
   */
  public void lpush(String key, String value) {
    sendBulk("LPUSH " + key, value);
  }

  /**
   * Return the length of the list stored at the specified key (List command).
   * If the key does not exist zero is returned (the same behaviour as for empty lists).
   * If the value stored at key is not a list an error is raised.
   */
  public int llen(String key) {
    return integer(sendInline("LLEN " + key));
  }

  /**
   * Return the specified elements of the list stored at the specified key (List command).
   * Start and end are zero-based indexes. 0 is the first element of the list (the list head), 1 the next element
   * and so on. For example <code>LRANGE foobar 0 2</code> will return the first three elements of the list.
   * <p/>
   * start and end can also be negative numbers indicating offsets from the end of the list. For example -1 is the
   * last element of the list, -2 the penultimate element and so on.
   * <p/>
   * Indexes out of range will not produce an error: if start is over the end of the list, or start > end, an empty
   * list is returned. If end is over the end of the list Redis will threat it just like the last element of the list.
   */
  public String[] lrange(String key, int start, int end) {
    return strings(sendInline("LRANGE " + key + " " + start + " " + end));
  }

  /**
   * Trim an existing list so that it will contain only the specified range of elements specified (List command).
   * Start and end are zero-based indexes. 0 is the first element of the list (the list head), 1 the next element
   * and so on. For example <code>LTRIM foobar 0 2</code> will modify the list stored at foobar key so that only
   * the first three elements of the list will remain.
   * <p/>
   * start and end can also be negative numbers indicating offsets from the end of the list. For example -1 is the
   * last element of the list, -2 the penultimate element and so on.
   * <p/>
   * Indexes out of range will not produce an error: if start is over the end of the list, or start > end, an empty
   * list is left as value. If end over the end of the list Redis will threat it just like the last element of the list.
   * <p/>
   * Hint: the obvious use of LTRIM is together with LPUSH/RPUSH. For example:
   * <pre>
   * LPUSH mylist &lt;someelement>
   * LTRIM mylist 0 99
   * </pre>
   * The above two commands will push elements in the list taking care that the list will not grow without limits.
   * This is very useful when using Redis to store logs for example. It is important to note that when used in this
   * way <code>LTRIM</code> is an O(1) operation because in the average case just one element is removed from the
   * tail of the list.
   */
  public void ltrim(String key, int start, int end) {
    sendInline("LTRIM " + key + " " + start + " " + end);
  }

  /**
   * Return the specified element of the list stored at the specified key (List command).
   * 0 is the first element, 1 the second and so on. Negative indexes are supported, for example -1 is the last element,
   * -2 the penultimate and so on.
   * <p/>
   * If the value stored at key is not of list type an error is returned.
   * If the index is out of range an empty string is returned.
   * <p/>
   * Note that even if the average time complexity is O(n) asking for the first or the last element of the list is O(1).
   */
  public String lindex(String key, int index) {
    return string(sendInline("LINDEX " + key + " " + index));
  }

  /**
   * Set the list element at index (see LINDEX for information about the index argument) with the new value (List
   * Command).
   * Out of range indexes will generate an error. Note that setting the first or last elements of the list is O(1).
   * Similarly to other list commands accepting indexes, the index can be negative to access elements starting from
   * the end of the list. So -1 is the last element, -2 is the penultimate, and so forth.
   */
  public void lset(String key, int index, String value) {
    sendBulk("LSET " + key + " " + index, value);
  }

  /**
   * Remove the first count occurrences of the value element from the list (List command).
   * If count is zero all the elements are removed. If count is negative elements are removed from tail to head,
   * instead to go from head to tail that is the normal behaviour. So for example <code>LREM</code> with count -2 and
   * <code>hello</code> as value to remove against the list <code>(a,b,c,hello,x,hello,hello)</code> will leave the list
   * <code>(a,b,c,hello,x)</code>. The number of removed elements is returned as an integer.
   * <p/>
   * Note that non existing keys are considered like empty lists by <code>LREM</code>, so <code>LREM</code> against non
   * existing keys will always return 0.
   */
  public int lrem(String key, int count, String value) {
    return integer(sendBulk("LREM " + key + " " + count, value));
  }

  /**
   * Atomically return and remove the first element of the list (List command).
   * For example if the list contains the elements <code>"a","b","c"</code> <code>LPOP</code> will return
   * <code>"a"</code> and the list will become <code>"b","c"</code>.
   * <p/>
   * If the key does not exist or the list is already empty, <code>null</code> is returned.
   */
  public String lpop(String key) {
    return string(sendInline("LPOP " + key));
  }

  /**
   * Atomically return and remove the last element of the list (List command).
   * For example if the list contains the elements <code>"a","b","c"</code> <code>RPOP</code> will return
   * <code>"c"</code> and the list will become <code>"a","b"</code>.
   * <p/>
   * If the key does not exist or the list is already empty, <code>null</code> is returned.
   */
  public String rpop(String key) {
    return string(sendInline("RPOP " + key));
  }

  /**
   * BLPOP is a blocking list pop primitive (List command).
   * You can see this commands as blocking versions of LPOP able to block if the specified keys don't exist or
   * contain empty lists.
   */
  public String[] blpop(String... keys) {
    return blpop(0, keys);
  }

  /**
   * BLPOP is a blocking list pop primitive (List command).
   * You can see this commands as blocking versions of LPOP able to block if the specified keys don't exist or
   * contain empty lists.
   */
  public String[] blpop(int seconds, String... keys) {
    checkNotEmpty(keys);
    return strings(sendInline("BLPOP " + join(keys, " ") + " " + seconds));
  }

  /**
   * BRPOP is a blocking list pop primitive (List command).
   * You can see this commands as blocking versions of RPOP able to block if the specified keys don't exist or
   * contain empty lists.
   */
  public String[] brpop(String... keys) {
    return blpop(0, keys);
  }

  /**
   * BLPOP is a blocking list pop primitive (List command).
   * You can see this commands as blocking versions of LPOP able to block if the specified keys don't exist or
   * contain empty lists.
   */
  public String[] brpop(int seconds, String... keys) {
    checkNotEmpty(keys);
    return strings(sendInline("BRPOP " + join(keys, " ") + " " + seconds));
  }

  /**
   * Atomically return and remove the last (tail) element of the srckey list, and push the element as the first (head)
   * element of the dstkey list (List command). For example if the source list contains the elements
   * <code>"a","b","c"</code> and the destination list contains the elements <code>"foo","bar"</code> after an RPOPLPUSH
   * command the content of the two lists will be <code>"a","b"</code> and <code>"c","foo","bar"</code>.
   *
   * If the key does not exist or the list is already empty <code>null</code> is returned. If the srckey and dstkey are
   * the same the operation is equivalent to removing the last element from the list and pusing it as first element of
   * the list, so it's a "list rotation" command.
   */
  public String rpoplpush(String srcKey, String dstKey) {
    return string(sendInline("RPOPLPUSH " + srcKey + " " + dstKey));
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Adds the specified member to the set value stored at key.
   * If member is already a member of the set no operation is performed.
   * If key does not exist a new set with the specified member as sole member is created.
   * If the key exists but does not hold a set value an error is thrown.
   * Return <code>true</code> if the new element was added and <code>false</code> otherwise.
   */
  public boolean sadd(String key, String member) {
    return bool(sendBulk("SADD " + key, member));
  }

  public boolean srem(String key, String member) {
    return bool(sendBulk("SREM " + key, member));
  }

  public String spop(String key) {
    return string(sendInline("SPOP " + key));
  }

  public boolean smove(String srckey, String dstkey, String member) {
    return bool(sendBulk("SMOVE " + srckey + " " + dstkey, member));
  }

  public int scard(String key) {
    return integer(sendInline("SCARD " + key));
  }

  public boolean sismember(String key, String member) {
    return bool(sendBulk("SISMEMBER " + key, member));
  }

  public String[] sinter(String... keys) {
    checkNotEmpty(keys);
    return strings(sendInline("SINTER " + join(keys, " ")));
  }

  public void sinterstore(String dstkey, String... keys) {
    checkNotEmpty(keys);
    sendInline("SINTERSTORE " + dstkey + " " + join(keys, " "));
  }

  public String[] sunion(String... keys) {
    checkNotEmpty(keys);
    return strings(sendInline("SUNION " + join(keys, " ")));
  }

  public void sunionstore(String dstkey, String... keys) {
    checkNotEmpty(keys);
    sendInline("SUNIONSTORE " + dstkey + " " + join(keys, " "));
  }

  public String[] sdiff(String... keys) {
    checkNotEmpty(keys);
    return strings(sendInline("SDIFF " + join(keys, " ")));
  }

  public void sdiffstore(String dstkey, String... keys) {
    checkNotEmpty(keys);
    sendInline("SDIFFSTORE " + dstkey + " " + join(keys, " "));
  }

  public String[] smembers(String key) {
    return strings(sendInline("SMEMBERS " + key));
  }

  public String srandmember(String key) {
    return string(sendInline("SRANDMEMBER " + key));
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Add the specified member having the specified score to the sorted set stored at key.
   * If member is already a member of the sorted set the score is updated, and the element reinserted in the right
   * position to ensure sorting. If key does not exist a new sorted set with the specified member as sole member
   * is created. If the key exists but does not hold a sorted set value an error is thrown.
   */
  public boolean zadd(String key, double score, String value) {
    return bool(sendBulk("ZADD " + key + " " + score, value));
  }

  /**
   * Remove the specified member from the sorted set value stored at key.
   * If member was not a member of the set no operation is performed.
   * If key does not not hold a set value an error is thrown.
   */
  public boolean zrem(String key, String member) {
    return bool(sendBulk("ZREM " + key, member));
  }

  /**
   * If member already exists in the sorted set adds the increment to its score and updates the position of the element
   * in the sorted set accordingly. If member does not already exist in the sorted set it is added with increment as
   * score (that is, like if the previous score was virtually zero). If key does not exist a new sorted set with the
   * specified member as sole member is created. If the key exists but does not hold a sorted set value an error is
   * thrown.
   */
  public double zincrby(String key, double offset, String member) {
    return Double.parseDouble(string(sendBulk("ZINCRBY " + key + " " + offset, member)));
  }

  /**
   * Return the rank of the member in the sorted set, with scores ordered from low to high.
   * When the given member does not exist in the sorted set, <code>null</code> is returned.
   * The returned rank (or index) of the member is 0-based.
   */
  public int zrank(String key, String member) {
    Object rank = sendBulk("ZRANK " + key, member);
    return rank == null ? -1 : integer(rank);
  }

  /**
   * Return the rank of the member in the ordered set, with scores ordered from high to low.
   * When the given member does not exist in the sorted set, <code>null</code> is returned.
   * The returned rank (or index) of the member is 0-based.
   */
  public int zrevrank(String key, String member) {
    Object rank = sendBulk("ZREVRANK " + key, member);
    return rank == null ? -1 : integer(rank);
  }

  /**
   * Return the specified elements of the sorted set at the specified key.
   * The elements are considered sorted from the lowerest to the highest score when using ZRANGE,
   * and in the reverse order when using ZREVRANGE. Start and end are zero-based indexes. 0 is the first element of the
   * sorted set (the one with the lowerest score when using ZRANGE), 1 the next element by score and so on.
   *
   * start and end can also be negative numbers indicating offsets from the end of the sorted set. For example -1 is the
   * last element of the sorted set, -2 the penultimate element and so on.
   *
   * Indexes out of range will not produce an error: if start is over the end of the sorted set, or start > end, an
   * empty list is returned. If end is over the end of the sorted set Redis will threat it just like the last element
   * of the sorted set.
   *
   * It's possible to pass the WITHSCORES option to the command in order to return not only the values but also the
   * scores of the elements. Redis will return the data as a single list composed of
   * <code>value1,score1,value2,score2,...,valueN,scoreN</code> but client libraries are free to return a more
   * appropriate data type (what we think is that the best return type for this command is a Array of two-elements
   * Array / Tuple in order to preserve sorting).
   */
  public String[] zrange(String key, int start, int end) {
    return strings(sendInline("ZRANGE " + key + " " + start + " " + end));
  }

  public static final class ScoredMember {
    private final String member;
    private final Double score;

    public ScoredMember(String member, Double score) {
      this.member = member;
      this.score = score;
    }

    public String getMember() {
      return member;
    }

    public Double getScore() {
      return score;
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this || obj instanceof ScoredMember &&
          ((ScoredMember) obj).member.equals(member) &&
          ((ScoredMember) obj).score.equals(score);
    }
  }

  public ScoredMember[] zrangeWithScores(String key, int start, int end) {
    String[] strings = strings(sendInline("ZRANGE " + key + " " + start + " " + end));
    ScoredMember[] scoredMembers = new ScoredMember[strings.length / 2];
    for (int i = 0; i < scoredMembers.length; i++) {
      scoredMembers[i] = new ScoredMember(strings[i * 2], new Double(strings[i * 2 + 1]));
    }
    return scoredMembers;
  }

  /**
   * Return the specified elements of the sorted set at the specified key.
   * The elements are considered sorted from the lowerest to the highest score when using ZRANGE,
   * and in the reverse order when using ZREVRANGE. Start and end are zero-based indexes. 0 is the first element of the
   * sorted set (the one with the lowerest score when using ZRANGE), 1 the next element by score and so on.
   *
   * start and end can also be negative numbers indicating offsets from the end of the sorted set. For example -1 is the
   * last element of the sorted set, -2 the penultimate element and so on.
   *
   * Indexes out of range will not produce an error: if start is over the end of the sorted set, or start > end, an
   * empty list is returned. If end is over the end of the sorted set Redis will threat it just like the last element
   * of the sorted set.
   *
   * It's possible to pass the WITHSCORES option to the command in order to return not only the values but also the
   * scores of the elements. Redis will return the data as a single list composed of
   * <code>value1,score1,value2,score2,...,valueN,scoreN</code> but client libraries are free to return a more
   * appropriate data type (what we think is that the best return type for this command is a Array of two-elements
   * Array / Tuple in order to preserve sorting).
   */
  public String[] zrevrange(String key, int start, int end) {
    return strings(sendInline("ZREVRANGE " + key + " " + start + " " + end));
  }

  public ScoredMember[] zrevrangeWithScores(String key, int start, int end) {
    String[] strings = strings(sendInline("ZREVRANGE " + key + " " + start + " " + end));
    ScoredMember[] scoredMembers = new ScoredMember[strings.length / 2];
    for (int i = 0; i < scoredMembers.length; i++) {
      scoredMembers[i] = new ScoredMember(strings[i * 2], new Double(strings[i * 2 + 1]));
    }
    return scoredMembers;
  }

  /**
   * Return the all the elements in the sorted set at key with a score between min and max (including elements with
   * score equal to min or max).
   *
   * The elements having the same score are returned sorted lexicographically as ASCII strings (this follows from a
   * property of Redis sorted sets and does not involve further computation).
   *
   * Using the optional LIMIT it's possible to get only a range of the matching elements in an SQL-alike way. Note
   * that if offset is large the commands needs to traverse the list for offset elements and this adds up to the
   * O(M) figure.
   * @return a list of elements in the specified score range
   */
  public String[] zrangebyscore(String key, double min, double max, int start, int end) {
    String cmd = "ZRANGEBYSCORE " + key + " " + min + " " + max;
    if (start != -1) {
      cmd += " LIMIT " + start + " " + end;
    }
    return strings(sendInline(cmd));
  }

  public String[] zrangebyscore(String key, double min, double max) {
    return zrangebyscore(key, min, max, -1, -1);
  }

  /**
   * Remove all elements in the sorted set at key with rank between start and end.
   * Start and end are 0-based with rank 0 being the element with the lowest score. Both start and end can be negative
   * numbers, where they indicate offsets starting at the element with the highest rank. For example: -1 is the element
   * with the highest score, -2 the element with the second highest score and so forth.
   * @return the number of elements removed
   */
  public int zremrangebyrank(String key, int start, int end) {
    return integer(sendInline("ZREMRANGEBYRANK " + key + " " + start + " " + end));
  }

  /**
   * Remove all the elements in the sorted set at key with a score between min and max (including elements with score
   * equal to min or max).
   * @return the number of elements removed
   */
  public int zremrangebyscore(String key, double min, double max) {
    return integer(sendInline("ZREMRANGEBYSCORE " + key + " " + min + " " + max));
  }

  /**
   * Return the sorted set cardinality (number of elements).
   * If the key does not exist 0 is returned, like for empty sorted sets.
   * @return the cardinality (number of elements) of the set as an integer.
   */
  public int zcard(String key) {
    return integer(sendInline("ZCARD " + key));
  }

  /**
   * Return the score of the specified member of the sorted set at key.
   * If the specified element does not exist in the sorted set, or the key does not exist at all, <code>null</code>
   * is returned.
   */
  public Double zscore(String key, String member) {
    Object result = sendBulk("ZSCORE " + key, member);
    return result == null ? null : new Double(string(result));
  }

  enum Aggregate {
    SUM("SUM"), MIN("MIN"), MAX("MAX");

    private final String value;

    Aggregate(String value) {
      this.value = value;
    }
  }

  public int zunion(String dstkey, String[] srckeys, double[] weights, Aggregate aggregate) {
    return zunionOrZinter("ZUNION", dstkey, srckeys, weights, aggregate);
  }

  public int zinter(String dstkey, String[] srckeys, double[] weights, Aggregate aggregate) {
    return zunionOrZinter("ZINTER", dstkey, srckeys, weights, aggregate);
  }

  private int zunionOrZinter(String cmd, String dstkey, String[] srckeys, double[] weights, Aggregate aggregate) {
    String optional = "";
    if (weights != null) {
      StringBuilder b = new StringBuilder(" WEIGHTS");
      for (double weight : weights) {
        b.append(" ").append(weight);
      }
      optional = b.toString();
    }
    if (aggregate != null) {
      optional += " AGGREGATE " + aggregate.value;
    }
    return integer(sendInline(cmd + " " + dstkey + " " + srckeys.length + " " + join(srckeys, " ") + optional));
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  public String hget(String key, String field) {
    return string(sendInline("HSET " + key + " " + field));
  }

  public boolean hset(String key, String field, String value) {
    return bool(sendBulk("HSET " + key + " " + field, value));
  }

  public boolean hsetnx(String key, String field, String value) {
    return bool(sendBulk("HSET " + key + " " + field, value));
  }

  public void hmset(String key, String... fieldsAndValues) {
    if (fieldsAndValues.length < 2 || (fieldsAndValues.length % 2) == 1) {
      throw new IllegalArgumentException();
    }
    sendMultiBulk("HMSET " + key, bytes(fieldsAndValues));
  }

  public int hincr(String key, String field) {
    return hincrby(key, field, 1);
  }

  public int hincrby(String key, String field, int offset) {
    return integer(sendInline("HINCRBY " + key + " " + field + " " + offset));
  }

  public int hdecr(String key, String field) {
    return hincrby(key, field, -1);
  }

  public int hdecrby(String key, String field, int offset) {
    return hincrby(key, field, -offset);
  }

  public boolean hexists(String key, String field) {
    return bool(sendInline("HEXISTS " + key + " " + field));
  }

  public boolean hdel(String key, String field) {
    return bool(sendInline("HDEL " + key + " " + field));
  }

  /**
   * Return the number of entries (fields) contained in the hash stored at key.
   * If the specified key does not exist, 0 is returned assuming an empty hash.
   */
  public int hlen(String key) {
    return integer(sendInline("HLEN " + key));
  }

  /**
   * Returns all the field names contained into a hash.
   */
  public String[] hkeys(String key) {
    return strings(sendInline("HKEYS " + key));
  }

  /**
   * Returns all the values contained into a hash.
   */
  public String[] hvals(String key) {
    return strings(sendInline("HVALS " + key));
  }

  /**
   * Returns both the fields and values in the form of field1, value1, field2, value2, ..., fieldN, valueN.
   */
  public String[] hgetall(String key) {
    return strings(sendInline("HGETALL " + key));
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  public String[] sort(String key) {
    return sort(key, null, -1, -1, null, false, false, null);
  }

  public String[] sort(String key, String byPattern, int start, int count, String getPattern, boolean desc, boolean alpha, String dstKey) {
    String cmd = "SORT " + key;
    if (byPattern != null) {
      cmd += " BY " + byPattern;
    }
    if (start != -1) {
      cmd += " LIMIT " + start + " " + count;
    }
    if (getPattern != null) {
      cmd += " GET " + getPattern;
    }
    if (desc) {
      cmd += " DESC";
    }
    if (alpha) {
      cmd += " ALPHA";
    }
    if (dstKey != null) {
      cmd += " STORE " + dstKey;
    }
    return strings(sendInline(cmd));
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  public String[] multi(Runnable r) {
    sendInline("MULTI");
    String[] answer = null;
    try {
      r.run(); // TODO commands return queues instead of real values 
      answer = strings(sendInline("EXEC"));
    } finally {
      if (answer == null) {
        sendInline("DISCARD");
        return null;
      }
    }
    return answer;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  public void subscribe(String... channels) {
    checkNotEmpty(channels);
    sendInline("SUBSCRIBE " + join(channels, " "));
  }

  public void psubscribe(String... patterns) {
    checkNotEmpty(patterns);
    sendInline("PSUBSCRIBE " + join(patterns, " "));
  }

  // TODO unsubscribe
  // TODO punsubscribe

  public int publish(String channel, String message) {
    return integer(sendBulk("PUBLISH " + channel, message));
  }

  /**
   * Returns the next message (a string array with three elements, the first being the message type, the second the
   * originating channel and the last one the payload) from a subscribed channel and blocks otherwise. The message type
   * is either "subscribe", "unsubscribe", or "message".
   */
  public String[] next() {
    try {
      return strings(handler.get().answer());
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Save the whole dataset on disk (this means that all the databases are saved, as well as keys with an EXPIRE set
   * (the expire is preserved). The server hangs while the saving is not completed, no connection is served in the
   * meanwhile.
   *
   * The background variant of this command is BGSAVE that is able to perform the saving in the background while
   * the server continues serving other clients.
   */
  public void save() {
    sendInline("SAVE");
  }

  /**
   * Save the DB in background. Redis forks, the parent continues to server the clients, the child saves the DB on
   * disk then exit. A client may be able to check if the operation succeeded using the LASTSAVE command.
   */
  public void bgsave() {
    sendInline("BGSAVE");
  }

  /**
   * BGREWRITEAOF rewrites the Append Only File in background when it gets too big. The Redis Append Only File is a
   * journal, so every operation modifying the dataset is logged in the Append Only File (and replayed at startup).
   * This means that the Append Only File always grows. In order to rebuild its content the BGREWRITEAOF creates a
   * new version of the append only file starting directly form the dataset in memory in order to guarantee the
   * generation of the minimal number of commands needed to rebuild the database.
   */
  public void bgrewriteaof() {
    sendInline("BGREWRITEAOF");
  }

  /**
   * Return the UNIX time of the last DB save executed with success. A client may check if a BGSAVE command succeeded
   * reading the LASTSAVE value, then issuing a BGSAVE command and checking at regular intervals every N seconds if
   * LASTSAVE changed.
   */
  public int lastsave() {
    return integer(sendInline("LASTSAVE"));
  }

  /**
   * Stop all the clients, save the DB, then quit the server. This commands makes sure that the DB is switched off
   * without the lost of any data. This is not guaranteed if the client uses simply "SAVE" and then "QUIT" because
   * other clients may alter the DB data between the two commands.
   */
  public void shutdown() {
    sendInline("SHUTDOWN");
  }

  /**
   * The info command returns different information and statistics about the server in an format that's simple to
   * parse by computers and easy to read by humans.
   */
  public String info() {
    return string(sendInline("INFO"));
  }

  /**
   * The SLAVEOF command can change the replication settings of a slave on the fly.
   * In the proper form SLAVEOF hostname port will make the server a slave of the specific server listening at the
   * specified hostname and port.
   *
   * If a server is already a slave of some master, SLAVEOF hostname port will stop the replication against the old
   * server and start the synchrnonization against the new one discarding the old dataset.
   */
  public void slaveOf(String host, int port) {
    sendInline("SLAVEOF " + host + " " + port);
  }

  /**
   * The SLAVEOF command can change the replication settings of a slave on the fly.
   * If a Redis server is arleady acting as slave, the command SLAVEOF NO ONE will turn off the replicaiton turning
   * the Redis server into a MASTER.
   *
   * The form SLAVEOF no one will stop replication turning the server into a MASTER but will not discard the
   * replication. So if the old master stop working it is possible to turn the slave into a master and set the
   * application to use the new master in read/write. Later when the other Redis server will be fixed it can be
   * configured in order to work as slave.
   */
  public void master() {
    sendInline("SLAVEOF no one");
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  private Object sendInline(String cmd) {
    return handler.get().sendInline(cmd);
  }

  private Object sendBulk(String cmd, String data) {
    return handler.get().sendBulk(cmd, data);
  }

  private Object sendMultiBulk(String cmd, byte[][] data) {
    return handler.get().sendMultiBulk(cmd, data);
  }

  /**
   * Converts the given String into a UTF-8-encoded <code>byte[]</code>.
   */
  private static byte[] bytes(String s) {
    try {
      return s.getBytes("utf-8");
    } catch (UnsupportedEncodingException e) {
      throw new Error(e);
    }
  }

  /**
   * Converts the given list of strings into a list of UTF-8-encoded <code>byte[]</code>s.
   */
  private static byte[][] bytes(String[] ss) {
    byte[][] bytes = new byte[ss.length][];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = bytes(ss[i]);
    }
    return bytes;
  }

  /**
   * Converts the given <code>byte[]</code> into a string, assuming UTF-8 encoding.
   * If <code>null</code> is passed, <code>null</code> is returned.
   */
  private static String string(Object o) {
    if (o == null) {
      return null;
    }
    try {
      return new String((byte[]) o, "utf-8");
    } catch (UnsupportedEncodingException e) {
      throw new Error(e);
    }
  }

  private static String[] strings(Object o) {
    if (o == null) {
      return null;
    }
    byte[][] datas = (byte[][]) o;
    String[] strings = new String[datas.length];
    for (int i = 0; i < strings.length; i++) {
      strings[i] = string(datas[i]);
    }
    return strings;
  }

  private static boolean bool(Object o) {
    return ((Integer) o) == 1;
  }

  private static int integer(Object o) {
    return (Integer) o;
  }

  /**
   * Takes an array of strings and returns a single joined string using the given separator.
   */
  private static String join(String[] strings, String separator) {
    if (strings.length == 0) {
      return "";
    }
    if (strings.length == 1) {
      return strings[0];
    }
    StringBuilder b = new StringBuilder(strings[0]);
    for (int i = 1; i < strings.length; i++) {
      b.append(separator);
      b.append(strings[i]);
    }
    return b.toString();
  }

  /**
   * Throws an <code>IllegalArgumentException</code> if the given array is <code>null</code> or empty.
   */
  private static void checkNotEmpty(Object[] elements) {
    if (elements == null || elements.length == 0) {
      throw new IllegalArgumentException();
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  public static class RuntimeIOException extends RuntimeException {
    private static final long serialVersionUID = -1811680483156468434L;

    RuntimeIOException(IOException e) {
      super(e);
    }
  }

  public static class RedisException extends RuntimeException {
    private static final long serialVersionUID = 8496183694840585146L;

    RedisException(String message) {
      super(message);
    }
  }
}
