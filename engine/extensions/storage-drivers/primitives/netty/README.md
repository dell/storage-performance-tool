# [Netty](https://netty.io/)-based Storage Driver

Netty uses its own I/O worker thread pool. The count of these threads is controlled by the `storage-driver-threads`
configuration option. However the connection pool allows to keep much more open connections at any moment of time. The
count of the open connections may be limited by `storage-driver-limit-concurrency` configuration option.

## 1. Configuration Reference

| Name                                           | Type         | Default Value    | Description                                      |
|:-----------------------------------------------|:-------------|:-----------------|:-------------------------------------------------|
| storage-net-node-addrs                         | List of strings | 127.0.0.1 | The list of the storage node IPs or hostnames to use for HTTP load. May include port numbers.
| storage-net-node-connAttemptsLimit             | Integer >= 0 | 0 | The limit for the subsequent connection attempts for each storage endpoint node. The node is excluded from the connection pool forever if the node has more subsequent connection failures. The default value (0) means no limit.
| storage-net-node-port                          | Integer > 0 | 7 | The common port number to access the storage nodes, may be overriden adding the port number to the storage-driver-addrs, for example: "127.0.0.1:9020,127.0.0.1:9022,..."
| storage-net-node-slice                         | Flag | false | Slice the storage node addresses between the spt nodes using the greatest common divisor or not
| storage-net-bindBacklogSize                    | Integer >= 0 | 0 |
| storage-net-interestOpQueued                   | Flag | false |
| storage-net-keepAlive                          | Flag | true |
| storage-net-linger                             | Integer >= 0 | 0 |
| storage-net-reuseAddr                          | Flag | true |
| storage-net-rcvBuf                             | Fixed size >= 0 | 0 | The network connection input buffer size. Estimated automatically if 0 (default)
| storage-net-sndBuf                             | Fixed size >= 0 | 0 | The network connection output buffer size. Estimated automatically if 0 (default)
| storage-net-selectInterval                     | Integer > 0 | 100 |
| storage-net-tcpNoDelay                         | Flag | true |
| storage-net-timeoutMilliSec                    | Integer >= 0 | 300000 | The socket/response idle timeout (milliseconds)
| storage-net-ioRatio                            | 0 < Integer < 100 | 50 | Internal [Netty's I/O ratio parameter](https://github.com/netty/netty/issues/1154#issuecomment-14870909). It's recommended to make it higher for large request/response payload (>1MB)
| storage-net-transport                          | Enum | nio | The I/O transport to use (see the [details](http://netty.io/wiki/native-transports.html)). By default tries to use "nio" (the most compatible). For Linux try to use "epoll" or "iouring", for MacOS/BSD use "kqueue" (requires rebuilding).
| storage-net-ssl-ciphers                        | List of strings | null | The list of ciphers to use if SSL is enabled. First cipher in the list that matches is applied. Make sure to match used ciphers and protocols. 
| storage-net-ssl-enabled                        | Flag | false | The flag to enable the load through SSL/TLS. Currently only HTTPS implementation is available. Have no effect if configured storage type is filesystem.
| storage-net-ssl-jsseProvider                   | String | BCJSSE | JSSE provider for PQC TLS. Used when `pqcMode` is `prefer` or `require`. Default resolves the Bouncy Castle JSSE provider without JVM-global registration.
| storage-net-ssl-namedGroups                    | List of strings | X25519MLKEM768, x25519, secp256r1 | TLS named groups (key exchange curves) offered during the handshake. The first group supported by the target is selected.
| storage-net-ssl-pqcMode                        | String | prefer | Post-quantum TLS mode: `off` (no PQC), `prefer` (attempt PQC with classical fallback), or `require` (fail if PQC provider unavailable).
| storage-net-ssl-protocols                      | List of strings | TLSv1.3, TLSv1.2 | The list of secure protocols to use if SSL is enabled 
| storage-net-ssl-provider                       | String | OPENSSL | The SSL provider. May be "OPENSSL" (better performance) or "JDK" (fallback)
| storage-net-writeSpinCount                     | Integer | 1 | Maximum loop count for a write operation until WritableByteChannel.write(ByteBuffer) returns a non-zero value.

## 2. Node Balancing

Spt uses the round-robin way to distribute I/O tasks if multiple storage endpoints are used.
Spt will try to distribute the active connections equally among the endpoints if a connection fails.

## 3. SSL/TLS

```bash
java -jar spt-<VERSION>.jar \
    --storage-net-ssl-enabled \
    --storage-net-node-port=9021 \
    ...
```

### Post-Quantum TLS (PQC)

When SSL is enabled, the driver defaults to `pqcMode=prefer`, which loads the Bouncy Castle JSSE provider and offers hybrid post-quantum key exchange groups (`X25519MLKEM768`, `x25519`, `secp256r1`) during the TLS 1.3 handshake. If the target or provider does not support PQC, the handshake falls back to classical TLS automatically.

To require PQC (fail if unavailable):

```bash
java -jar spt-<VERSION>.jar \
    --storage-net-ssl-enabled \
    --storage-net-ssl-pqcMode=require \
    ...
```

To disable PQC entirely:

```bash
java -jar spt-<VERSION>.jar \
    --storage-net-ssl-enabled \
    --storage-net-ssl-pqcMode=off \
    ...
```

See [`cli/docs/PQC_TLS.md`](../../../cli/docs/PQC_TLS.md) for the full guide including verification, troubleshooting, and driver coverage.

## 4. Connection Timeout

```bash
java -jar spt-<VERSION>.jar \
    --storage-net-timeoutMilliSec=300000 \
    ...
```

## 5. I/O Buffer Size

Spt automatically adopts the input and output buffer sizes depending on the step info. For
example, for *create* I/O type the input buffer size is set to the minimal value (4KB) and the
output buffer size is set to configured data item size (if any). If *read* I/O type is used the
behavior is right opposite - specific input buffer size and minimal output buffer size. This
improves the I/O performance significantly. But users may set the buffer sizes manually.

Example: setting the *input* buffer to 100KB:
```bash
java -jar spt-<VERSION>.jar \
    --storage-net-rcvBuf=100KB \
    ...
```

Example: setting the *output* buffer to 10MB:
```bash
java -jar spt-<VERSION>.jar \
    --storage-net-sndBuf=10MB \
    ...
```
