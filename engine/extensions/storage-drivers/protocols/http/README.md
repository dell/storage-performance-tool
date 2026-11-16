# HTTP Storage Driver

## 1. Configuration Reference

| Name                                           | Type         | Default Value    | Description                                      |
|:-----------------------------------------------|:-------------|:-----------------|:-------------------------------------------------|
| storage-net-http-headers                       | Map          | { "Connection" : "keep-alive", "Date": "#{date:formatNowRfc1123()}%{date:formatNowRfc1123()}", "User-Agent" : "spt-storage-driver-http/4.2.6" } | Custom HTTP headers section. A user may place here a key-value pair which will be used as HTTP header. The headers will be appended to every HTTP request issued. In a YAML or JSON defaults document, or `spt run --engine-override storage.net.http.headers.<name>=<value>`, a number or boolean value is sent as written (`X-Id: 007` sends `007`); a header with no value, or a list or map value, fails the step with a message that names it.
| storage-net-http-uri-args                      | Map          | {}               | Custom URI query arguments according [RFC 2396](http://www.ietf.org/rfc/rfc2396.txt). The arguments are appended to the requests listed in [3. Custom URI Arguments](#3-custom-uri-arguments), not to every HTTP request. In a YAML or JSON defaults document, or `spt run --engine-override storage.net.http.uri.args.<name>=<value>`, a number or boolean value is sent as written (`tid: 0012` sends `tid=0012`); a list or map value fails the step with a message that names it, and an argument with no value is sent as `name=null`.
| storage-net-http-read-metadata-only            | Map          | false            | Specifies whether Spt issues GET request or HEAD. HEAD is used when enabled.
| storage-net-http-max-chunk-size                | Integer      | 65536            | The limit, in bytes, at which Netty will send a chunk down the pipeline.

## 2. Custom HTTP Headers

Scenario example:
```javascript
var customHttpHeadersConfig = {
    "storage" : {
        "net" : {
            "http" : {
                "headers" : {
                    "header-name-0" : "header_value_0",
                    "header-name-1" : "header_value_1",
                    // ...
                    "header-name-N" : "header_value_N"
                }
            }
        }
    }
};

Load
    .config(customHttpHeadersConfig)
    .run();
```

**Note**:
> Don't use the command line arguments for the custom HTTP headers setting.

### 2.1. Expressions

Scenario example, note both the parameterized header name and value:
```javascript
var varHttpHeadersConfig = {
    "storage" : {
        "net" : {
            "http" : {
                "headers" : {
                    "x-amz-meta-${math:random(30) + 1}" : "${date:format("yyyy-MM-dd'T'HH:mm:ssZ").format(date:from(rnd.nextLong(time:millisSinceEpoch())))}"
                }
            }
        }
    }
};

Load
    .config(varHttpHeadersConfig)
    .run();
```

## 3. Custom URI Arguments

Custom URI query arguments may be set in the same way as custom HTTP headers.

With the `s3` and `s3-rdma` drivers, the arguments are sent on:

- object requests built by the common request builder: PUT, GET, HEAD, UPDATE, copy, and DELETE when it is not a standalone DELETE step;
- the bucket HEAD and the bucket create that check and create the target bucket.

They are not sent on multipart upload requests (initiate, part, complete, abort), range-read part requests, object versioning or tagging requests, standalone DELETE requests, LIST requests, item-source listing, the delimiter probe, bucket versioning requests, the presence `HEAD` of a verification, or multi-object delete. With object versioning or object tagging enabled, they are not sent on object requests, except a DELETE or UPDATE of a composite (multipart) item, which still takes the common builder and carries them.

The `s3-tables` driver builds its table requests itself and sends no arguments on them; only its target-bucket HEAD and create carry them.

With `--storage-auth-version 4`, the arguments are part of the signed query, sorted by name and then by value. With V2, object requests are signed over the path only, so an argument that V2 signs as a sub-resource (such as `acl` or `versionId`) is not signed.

Argument names and values are sent as written, without encoding: give values that are already URI-safe, and percent-encode any other character yourself. Write a space as `%20` and a literal plus as `%2B`: a raw `+` is signed as a plus, and a target that reads `+` in a query as a space will refuse the signature.

```javascript
var uriQueryConfig = {
    "storage" : {
        "net" : {
            "http" : {
                "uri" : {
                    "args" : {
                        "foo": "bar",
                        "key1" : "val1"
                    }
                }
            }
        }
    }
};

Load
    .config(uriQueryConfig)
    .run();
```

will produce the HTTP requests with URIs like:
`/20190306.104255.627/kticoxcknpuy?key1=val1&foo=bar`


**Note**:
> Don't use the command line arguments for the custom HTTP URI query arguments setting.

### 3.1. Expressions

Example:
```javascript
var uriQueryConfig = {
    "storage" : {
        "net" : {
            "http" : {
                "uri" : {
                    "args" : {
                        "foo${rnd.nextInt()}" : "bar${time:millisSinceEpoch()}",
                        "key1" : "${date:formatNowIso8601()}",
                        "${e}" : "${pi}"
                    }
                }
            }
        }
    }
};

Load
    .config(uriQueryConfig)
    .run();
```

will produce the HTTP requests with URIs like:
`/20190306.104255.627/kticoxcknpuy?key1=2019-03-06T10:42:56,768&2.718281828459045=3.141592653589793&foo1130828259=bar1551868976768`

**Note**:
> Don't use both synchronous and asynchronous expressions for the query args simultaneously. All configured query args
> are collected into the single expression input.
