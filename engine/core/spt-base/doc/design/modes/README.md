# Modes

Spt can be launched in 2 main modes:
* **Standalone** mode
* **Distributed** mode

## Standalone mode

![Standalone Mode](../../images/standalone_mode.png)

This is the easiest way to use spt. In this case, one spt instance is launched and the load on the storage generated from it. This mode **doesn't require additional parameters compared to additional parameters required for distributed mode**.

## Distributed mode

![Distributed Mode](../../images/distributed_mode.png)

This mode is used for distributed load on the storage. This mode is used **not** for a **distributed storage** (this function is also supported in a standalone mode), but for **several instances of the spt**.
This mode is used to increase the load on the storage. 
[Detailed documentation](distributed_mode)


# Other modes:

* [Recycle mode](recycle_mode)
* [Copy mode](copy_mode)
