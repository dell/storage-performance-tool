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
1. Extract the ZIP to your preferred location.
2. Run `./run.sh --help` (Linux/macOS) or `run.bat --help` (Windows) to verify the installation.
3. Copy or author scenarios under `scenarios/`, or mount them into the container when using Docker.

Upgrade & Compatibility Notes
-----------------------------
- CLI 5.x expects to pair with Engine 5.x; patch releases remain compatible within the same minor line.
- To pin a specific engine patch, note the version above and pass `--engine-version` (or set `SPT_ENGINE_TAG`) in the CLI.

Additional Resources
--------------------
- Documentation: https://github.com/dell/storage-performance-tool
- Issue tracker: https://github.com/dell/storage-performance-tool/issues
