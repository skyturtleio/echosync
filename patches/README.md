# sql-pg 4.1.5 interrupted acquisition

`sql-pg-4.1.5.patch` repairs an upstream connection-owner permit leak. Interrupting
`pinExclusive` after taking the permit but before installing cleanup can leave an
idle pooled connection unusable. EchoSync's public Pull and Execute both expose this
failure with a size-one pool. The patch restores scoped ownership without masking
transaction bodies or changing EchoSync's engine behavior.

The reviewed patch has SHA256
`e90648169b0a63d05b25a2eae06900a0234716fbe7983da001ec67126739f80e`.
It changes both `src/PgConnection.ts` and the executed `dist/PgConnection.js`.
Failed acquisition closes a child scope immediately; successful acquisition keeps
the permit until caller scope close, releasing the pinned view before the permit.
Upstream 4.1.5 leaves `pinExclusive` unchanged, so the patch is byte-identical to the
4.1.4 one. Upstream's only `PgConnection` change, `streamRows` taking the wire with
`takeScoped`, still closes wire, pinned view, then permit in that order.

Bun applies this patch through this repository's `patchedDependencies`. **Consumers
must configure the same patch at their own install root**; a dependency's patch
configuration does not patch the application's shared driver automatically.
Do not substitute an unpatched registry driver while retaining Effect 4.1.5.

After a clean frozen install, the patched file SHA256 values are:

- Source: `a615b7faf909fe72d2de7667b5171d58b2cfe1f8796e88b2611a1861e5cc38ac`
- Runtime: `62fdf761c942a966361a9421b27dac7d0b3bdd5bf3c01eaf18b0d64d47e05229`

Validation interrupts public Pull, public Execute and a driver transaction at each
scheduler evaluation boundary, then requires an uncancelled query, transaction and
public Pull to succeed; the PostgreSQL and browser invariant suites also pass. Driver-level controls cover
contended acquisition, pipeline drain and acquisition timeout; unpatched 4.1.5 fails
the Pull, Execute, contended and drain sweeps. These establish the tested ownership
behavior, not production incident frequency or performance.

A separate upstream defect remains in 4.1.4 and 4.1.5, with or without this patch:
interrupting a row stream (`.stream`) after it registers its abort-drain finalizer
but before it registers as the connection consumer leaves that finalizer waiting,
uninterruptibly, for a reply to a query never sent. EchoSync does not use row streams.
With that window closed in a scratch install, stream sweeps pass under this patch.
Reassess this patch against the upstream fix before upgrading the driver again.
