# sql-pg 4.1.4 interrupted acquisition

`sql-pg-4.1.4.patch` repairs an upstream connection-owner permit leak. Interrupting
`pinExclusive` after taking the permit but before installing cleanup can leave an
idle pooled connection unusable. EchoSync's public Pull and Execute both expose this
failure with a size-one pool. The patch restores scoped ownership without masking
transaction bodies or changing EchoSync's engine behavior.

The reviewed patch has SHA256
`e90648169b0a63d05b25a2eae06900a0234716fbe7983da001ec67126739f80e`.
It changes both `src/PgConnection.ts` and the executed `dist/PgConnection.js`.
Failed acquisition closes a child scope immediately; successful acquisition keeps
the permit until caller scope close, releasing the pinned view before the permit.

Bun applies this patch through this repository's `patchedDependencies`. **Consumers
must configure the same patch at their own install root**; a dependency's patch
configuration does not patch the application's shared driver automatically.
Do not substitute an unpatched registry driver while retaining Effect 4.1.4.

After a clean frozen install, the patched file SHA256 values are:

- Source: `88486f9375dfc7b64e5293f728dfc909690e47807fc09f7d0543f2c6668e21fc`
- Runtime: `79c13c56e0218325578f3dc56c5207082f859403e99e37c02f15ad51e028ca7a`

Validation includes interruption at each scheduler evaluation boundary followed by
an uncancelled query and public Pull, plus the PostgreSQL and browser invariant
suites. Upstream-only controls also cover contended acquisition, pipeline drain,
live caller scopes, repeated close, and acquisition timeout. These establish the
tested ownership behavior, not production incident frequency or performance.
Reassess this patch against the upstream fix before upgrading the driver again.
