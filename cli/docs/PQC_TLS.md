# Post-Quantum Cryptography (PQC) TLS

SPT supports post-quantum TLS handshakes on the Netty HTTPS path, allowing you to benchmark storage targets with PQC-capable key exchange. This is useful for measuring potential overhead from PQC negotiation/decode paths on storage infrastructure.

PQC support covers the `default` (Netty), `rdma`, and S3 Tables drivers — all of which share the Netty TLS stack. The `aws` (AWS SDK) driver is not covered in this release.

> **No new CLI flags required.** PQC is enabled by default in `prefer` mode. When an HTTPS endpoint is used, SPT automatically offers hybrid post-quantum key exchange groups and falls back to classical TLS if the target does not support them.

---

## How It Works

When SSL is enabled (HTTPS endpoints), the Netty driver:

1. Loads the [Bouncy Castle JSSE](https://www.bouncycastle.org/java.html) provider (`BCJSSE`) as a per-context TLS provider — no JVM-global provider mutation.
2. Offers hybrid key exchange groups during the TLS 1.3 handshake: `X25519MLKEM768` (post-quantum hybrid), `x25519`, and `secp256r1` (classical fallbacks).
3. Logs the negotiated TLS protocol and cipher suite once per driver instance at INFO level.

The behavior is controlled by `pqcMode`:

| Mode | Behavior |
|------|----------|
| `prefer` (default) | Attempt PQC handshake. If the JSSE provider is unavailable or the target rejects the named groups, fall back to standard TLS with a one-time warning. |
| `require` | PQC handshake is mandatory. If the JSSE provider is unavailable, the driver fails fast with a configuration error. |
| `off` | PQC is disabled. The driver uses its existing non-PQC TLS path. |

---

## Engine Configuration

PQC settings live under `storage.net.ssl` in the Netty extension config schema. These are engine-level settings with sensible defaults — most users do not need to change them.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `storage-net-ssl-pqcMode` | string | `prefer` | PQC TLS mode: `off`, `prefer`, or `require` |
| `storage-net-ssl-jsseProvider` | string | `BCJSSE` | JSSE provider name for PQC TLS |
| `storage-net-ssl-namedGroups` | list | `X25519MLKEM768, x25519, secp256r1` | TLS named groups (key exchange curves) |

These can be overridden via engine CLI flags using confuse's dash-separated path resolution. For example, to force `require` mode:

```
--storage-net-ssl-pqcMode=require
```

Or to disable PQC entirely:

```
--storage-net-ssl-pqcMode=off
```

The defaults also update `storage-net-ssl-protocols` to `TLSv1.3, TLSv1.2` (TLS 1.3 preferred, with 1.2 fallback).

---

## Verifying PQC Negotiation

SPT logs the negotiated TLS protocol and cipher suite once per driver instance. Look for this line in engine logs:

```
negotiated TLS protocol=TLSv1.3, cipher=TLS_AES_128_GCM_SHA256
```

Additional initialization logs at INFO level show the active configuration:

```
SSL/TLS PQC mode: prefer
SSL/TLS JSSE provider: BCJSSE
SSL/TLS named groups: X25519MLKEM768, x25519, secp256r1
SSL/TLS protocols: TLSv1.3, TLSv1.2
```

If the BCJSSE provider is unavailable in `prefer` mode, you will see a one-time warning:

```
PQC TLS provider "BCJSSE" is unavailable; falling back to default JSSE provider
```

---

## Driver Coverage

| Driver | PQC Support | Notes |
|--------|-------------|-------|
| `default` (Netty) | Yes | Full PQC TLS with BCJSSE provider |
| `rdma` | Yes | Shares Netty control-plane TLS path |
| `aws` | No | AWS SDK driver — deferred to a future release |

---

## Named Groups

The default named groups offer a hybrid-first negotiation strategy:

| Group | Type | Purpose |
|-------|------|---------|
| `X25519MLKEM768` | Post-quantum hybrid | X25519 + ML-KEM-768. Provides quantum-resistant key exchange with classical fallback in one round-trip. |
| `x25519` | Classical | Curve25519. Widely supported, strong classical security. |
| `secp256r1` | Classical | NIST P-256. Broadest compatibility fallback. |

The target selects the first group it supports from the client's offered list. If the target does not support `X25519MLKEM768`, the handshake proceeds with `x25519` or `secp256r1`.

---

## Troubleshooting

### PQC provider unavailable

If you see `PQC TLS provider "BCJSSE" is unavailable` warnings:

- In `prefer` mode, this is informational — TLS proceeds with the default JDK provider.
- In `require` mode, this is a fatal error. Verify that the Bouncy Castle JARs (`bcprov-jdk18on`, `bctls-jdk18on`, `bcutil-jdk18on`) are present in the engine's runtime classpath. If using a custom Docker image, ensure the BC dependencies are included in the Netty extension.

### Named groups not applied

A one-time warning `failed to apply SSL named groups` means the JSSE provider does not support the configured groups. In `prefer` mode, the handshake continues without the specified groups. In `require` mode, this throws an error.

### TLS 1.2 negotiated instead of 1.3

The target may not support TLS 1.3. Check target-side TLS configuration. PQC key exchange requires TLS 1.3 — if TLS 1.2 is negotiated, the handshake used classical key exchange regardless of the named groups configuration.
