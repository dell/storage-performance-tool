SPT Engine Bundle
=================

Version: ${version}

Contents
--------
- `spt.jar` – core engine executable JAR.
- `ext/` – shipped extensions automatically linked at runtime.
- `scenarios/` – example workload scenarios.
- `run.sh` / `run.bat` – convenience launchers.

Getting Started
---------------
1. Extract the ZIP to your preferred location. The launchers run the `java` on PATH, which must be
   Java 21 or later; the SPT image runs Java 25.
2. Run `./run.sh --help` (Linux/macOS) or `run.bat --help` (Windows) to verify the installation.
3. Copy or author scenarios under `scenarios/`, or mount them into the container when using Docker.

JVM Options
-----------
- `run.sh` and `run.bat` start the JVM with the SPT image's default options, listed in `run.sh`,
  with two differences: they leave out `-XX:+UseCompactObjectHeaders`, which Java 21 refuses, and
  they pass `-XX:UseAVX=2` only when the `java` they run accepts it, as only x86 JVMs do.
- `SPT_JAVA_OPTS` adds options after the defaults, and when an option is given twice, the JVM uses
  the later one: `SPT_JAVA_OPTS="-Xmx8g" ./run.sh ...` keeps every other default. To pick another
  garbage collector, turn ZGC off first: `SPT_JAVA_OPTS="-XX:-UseZGC -XX:+UseG1GC"`. A maximum
  heap below 1 GiB needs a matching `-Xms`. The launchers don't read `JAVA_OPTS`. To run without
  the defaults, call `java <options> -jar spt.jar` directly.
- Unless `GLIBC_TUNABLES` is set, even to empty, `run.sh` sets it to `glibc.malloc.hugetlb=1`, as
  the image does, to ask glibc's malloc for huge pages. glibc before 2.35 and musl ignore it.
- `run.sh` replaces itself with the JVM, so a signal sent to it stops the engine. To find the
  engine's process, match `spt.jar`, not `run.sh`.

Upgrade & Compatibility Notes
-----------------------------
- CLI 5.x expects to pair with Engine 5.x; patch releases remain compatible within the same minor line.
- To pin a specific engine patch, note the version above and pass `--engine-version` (or set `SPT_ENGINE_TAG`) in the CLI.

Additional Resources
--------------------
- Documentation: https://github.com/dell/storage-performance-tool
- Issue tracker: https://github.com/dell/storage-performance-tool/issues
