# Spt CLI arguments

GNU style command line arguments are used:

```bash
java spt-<VERSION>.jar \
    --read \
    --item-input-file=items.csv \
    --storage-driver-limit-concurrency=10 \
    --storage-auth-uid=user1 \
    --storage-auth-secret=ChangeIt \
    --storage-net-node-addrs=10.20.30.40,10.20.30.41
```

The command-line options are directly mapped to the **configuration** items. For example the configuration option
`item-data-size` corresponds to the CLI argument `--item-data-size`.

[More about configuration](../configuration)


### Limitations

It's not possible to use the command line to specify **map/dict** type options, a user should use the scenario file for this.

* [Example for http header](https://github.com/dell-spt/spt-storage-driver-http#2-custom-http-headers)
* [Example for object tagging](https://github.com/dell-spt/spt-storage-driver-s3#421-put-object-tags)
