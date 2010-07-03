Simple [Redis][1] client library for Java 6. 

Supports version 2.0.0 with the exception of PUB/SUB.

Commands are named according to the [Command Reference][2] even if that is not way methods would be named in Java.
I took the liberty to copy some documentation over, so some parts of the Javadoc are probably copyright (c) 2006-2010,
Salvatore Sanfilippo and other Wiki contributors.

[1]:http://code.google.com/p/redis/
[2]:http://code.google.com/p/redis/wiki/CommandReference

Example usage
-------------
    RedisClient client = new RedisClient("localhost", RedisClient.DEFAULT_PORT);
    try {
      client.set("key", "some value");
      System.out.println(client.get("key"));
    } finally {
      client.close();    
    }

The client is thread-safe as it automatically creates a new connection for a new thread, storing a set of handlers in
a `ThreadLoad` variable.

*PLEASE NOTE:* Redis uses (8-bit clean) byte arrays for keys and values. For convenience, this client uses Strings
instead which are then UTF-8 encoded. It still uses the old wire protocol which requires that keys do not contain
spaces.

TODO
----
* Add a Maven POM and change package name to something more standard.
* Support binary values, make use of collections
* Think about PUB/SUB

License
-------
Copyright (c) 2010 Stefan Matthias Aust

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit
persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.