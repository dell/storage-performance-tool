# Copy Mode

# Contents

1. [Limitations](#1-limitations)<br/>
2. [Approach](#2-approach)<br/>
2.1. [General](#21-general)<br/>
2.2. [Filesystem Storage Case](#22-filesystem-storage-case)<br/>
2.3. [HTTP Storage Case](#23-http-storage-case)<br/>
3. [Configuration](#3-configuration)<br/>

The copy mode functionality allows to perform a copy operation on the multiple files instead of write one. The
performance rates may be significantly different for the copy and write operations. Some cloud storage APIs also support
copying the objects (S3 and Swift) so the functionality may be a general. In case of S3 and Swift there's no payload
sent while copying the objects so these requests may be significantly faster than writing new objects.

## 1. Limitations

1. Works only if the input path is different from the output one. A copy names an object's destination by its
   listed name whenever that name already lies under the output path, so an overlapping output path can make a
   destination equal to its source. The engine checks a constant output path against a listed `--item-input-path`
   when the step starts; it can't check an output path expression or an items file (`--item-input-file`).
   * Storage drivers other than `fs` compare the two paths without their leading and trailing slashes (`/src`,
     `src` and `/src/` are the same path).
     * An output path equal to the input path copies every object onto itself, so the step fails at start (exit 1).
     * An output path under the input path, such as `/src` to `/src/dir`, works on a first run into an empty prefix.
       But the listing also returns the keys already under the output path, such as an earlier run's copies, and
       those are copied onto themselves and counted as successes. The step logs one warning at start and runs.
   * On `fs` the paths compare as written: a path without a leading slash is relative to the working directory,
     and `.` and `..` segments name other directories. An output path equal to the input path, or one of its parent
     directories, such as `/data/src` to `/data` or `/data/src/`, used to open each listed source file as its own
     destination, empty it, and never finish. The engine now refuses it at start (exit 1). An output path under the
     input path is fine on `fs`, because a listing does not descend into subdirectories. A relative input path is
     not checked at start.
2. Copying the containers (Swift) is not supported
3. Copying the buckets (S3) is not supported
4. Copying the objects (Atmos) while using /rest/object interface is not supported yet

## 2. Approach

### 2.1. General

Copy mode is enabled if:
* `--load-op-type` option is set to `create` (this is by default) ***and***
* `--item-input-path` is set to existing bucket/container/directory

`--item-input-file` may name the items to copy, but it does not enable copy mode by itself: without
`--item-input-path`, a `create` step writes new data for the listed items.

### 2.2. Filesystem Storage Case

In case of copying the filesystem directories there's a size may be
calculated, so there's a size and bandwidth (MiB/sec) metrics
are available.

A listed item, or an item from an items file written by an `fs` step, is named by the source file's full path,
so it lands under the output path with that full path, as for `s3`: `/src/dir/f0` is copied to
`/dst/src/dir/f0`. Missing directories under the output path are created.

A listed path covers one directory level: its subdirectories are not copied. A subdirectory entry fails and leaves
an empty file in its place under the output path, so a later copy of that subdirectory to the same output path
fails. Copy nested directories deepest first, or to separate output paths.

### 2.3. HTTP Storage Case

> Note:
Cloud Storage object copy requests don't contain any payload. The
byte count related metrics report the copied items' sizes, the logical
size copied, not the bytes on the wire.

#### 2.3.1. S3 Objects Copying

The source object path is specified with `x-amz-copy-source` header.

With `--item-input-path`, the `s3` and `s3-aws` drivers both land a copy under the source bucket's name: `/src/k0` is copied to `/dst/src/k0`, and a nested key such as `dir/k0` to `/dst/src/dir/k0`. So count the copies under the `<source bucket>/` prefix of the target bucket.

The same holds for listed items and for an items file whose names carry a bucket, such as one written by an earlier READ or copy step: an item named `/src/dir/k0` lands at `/dst/src/dir/k0`.

`s3-aws` copies the listed items only when the path is a bucket, `/src`. A path that names an object, such as `/src/k0`, copies that one object to every item.

#### 2.3.2. Swift Objects Copying

There are two variants of object copy requests:

* Using HTTP method `COPY` and `Destination` header.
* Using HTTP method `PUT` and `X-Copy-From` header.

The 2nd variant is preferred ss far as COPY HTTP method is not standard.

The source object URI is specified with `X-Copy-From` header.

## 3. Configuration

In order to perform a copy load step it's necessary:

* Use `create` load type.
* Specify `--item-input-path` (the source container/bucket/directory, contains the items to copy) to a proper value.
  `--item-input-file` may also be given to name the items to copy; without `--item-input-path` it writes generated
  data instead of copying (see [2.1](#21-general)).
* Specify `--item-output-path` (the target container/bucket/directory) to a proper value.

For details, see the example scenario located at:
`example/scenario/js/types/additional/copy_load_using_env_vars.js` in the Spt home directory.
