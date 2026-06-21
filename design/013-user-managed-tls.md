# 013 — User-Managed TLS Certificates

**Status:** done
**Milestone:** M11

Allow operators to bring their own TLS cert/key pairs for instance mTLS and client-facing TLS, with independent overrides for each of the four certificate secret fields.

**Goal:** Allow operators to bring their own TLS certificate/key pairs for
instance mTLS and client-facing TLS instead of relying solely on
operator-generated cert-manager certificates. Support independent overrides for
each of the four existing secret fields, wire the currently-dead
`ServerAltDNSNames`, fix the all-or-nothing gate and the `CASecretName` overwrite
bug, and validate user-provided secrets.

## Why

The `CertificatesConfiguration` type already exists with four override fields
(`ServerCASecret`, `ServerTLSSecret`, `ClientCASecret`, `ReplicationTLSSecret`)
plus `ServerAltDNSNames` — but the implementation has bugs and gaps:

1. **All-or-nothing gate**: `hasUserCertificates()` requires all three secrets
   before skipping cert-manager. A user who brings only a server cert but wants
   the operator to manage the CA is forced into full operator generation.
2. **`CASecretName` overwrite bug**: `ClientCASecret` overwrites the
   `ServerCASecret` value in `plan.CASecretName` because they share one field.
   The server CA and client CA are logically the same CA in the simple model,
   but user overrides must be independent.
3. **`ServerAltDNSNames` dead code**: Defined but never threaded into
   cert-manager Certificate DNS names.
4. **No user-secret validation**: User-provided secrets are never checked for
   existence, TLS type, or valid PEM structure before being mounted.

## Scope

### In scope

- **Independent secret overrides**: Each of the four fields
  (`ServerCASecret`, `ServerTLSSecret`, `ClientCASecret`, `ReplicationTLSSecret`)
  can be set independently. The operator generates cert-manager resources only
  for the unset fields.
- **Server CA / Client CA separation**: `plan.ServerCASecretName` and
  `plan.ClientCASecretName` become distinct fields. By default both are
  `{cluster}-ca` (the operator-generated CA). User overrides are independent.
- **`ServerAltDNSNames`**: Wired into cert-manager Certificate DNS names
  alongside the auto-generated SANs.
- **User-secret validation**: When the user provides a secret reference, the
  operator validates it exists, is `kubernetes.io/tls` type (or `Opaque` for CA
  bundles), and contains valid PEM data.
- **`CertificatesStatus` inline**: Embed `CertificatesConfiguration` into status
  and populate it (CNPG parity).
- **Unit + e2e coverage**.

### Out of scope

- Automatic certificate renewal (cert-manager handles this; we don't add our
  own renewal logic like CNPG's `renewCASecret`).
- Certificate expiry monitoring/alerts (works through cert-manager Certificate
  status and can be observed externally).
- Supporting non-TLS secrets for CA bundles (we accept `kubernetes.io/tls` or
  `Opaque` with `ca.crt` key for CA secrets; PEM-only validation).

## Current bugs fixed

### Bug 1: All-or-nothing gate (cluster_pki.go:111)

`hasUserCertificates()` currently returns true only when ALL three fields are
non-empty. The operator then skips all cert-manager generation. Fix: each field
controls its own cert-manager resource independently.

### Bug 2: CASecretName overwrite (cluster_plan.go:229-240)

`ClientCASecret` overwrites `CASecretName` because they share one field. Fix:
split into `ServerCASecretName` and `ClientCASecretName`.

## API changes

### CertificatesConfiguration (existing, no schema changes)

```go
type CertificatesConfiguration struct {
    ServerCASecret       string   // CA for server certs (cert-manager CA Issuer)
    ServerTLSSecret      string   // kubernetes.io/tls server cert per instance
    ReplicationTLSSecret string   // kubernetes.io/tls client cert for replication
    ClientCASecret       string   // CA for client certs (mounted as client-ca)
    ServerAltDNSNames    []string // additional SANs for server certs
}
```

No new fields. All four already exist. Only behavioral changes.

### CertificatesStatus (amended to inline config)

```go
type CertificatesStatus struct {
    // CertificatesConfiguration mirrors the spec certificates configuration in
    // status so users can see the resolved secret names.
    CertificatesConfiguration `json:",inline"`

    // Expirations reports certificate expiry times keyed by secret name.
    // +optional
    Expirations map[string]string `json:"expirations,omitempty"`
}
```

Add `Expirations` and inline the configuration for CNPG parity.

## Controller changes

### cluster_plan.go — Split CA fields

```go
type clusterPlan struct {
    // ...
    // ServerCASecretName is the CA secret used by the cert-manager CA Issuer
    // to sign server certificates.
    ServerCASecretName string
    // ClientCASecretName is the CA secret mounted as client-ca for verifying
    // client certificates.
    ClientCASecretName string
    // ...
}
```

**`buildPlan` changes:**

```
ServerCASecretName = cluster.Name + "-ca"
ClientCASecretName = cluster.Name + "-ca"

if certs != nil {
    if certs.ServerCASecret != "" {
        plan.ServerCASecretName = certs.ServerCASecret
    }
    if certs.ClientCASecret != "" {
        plan.ClientCASecretName = certs.ClientCASecret
    }
    if certs.ServerTLSSecret != "" {
        plan.UserServerTLSSecret = certs.ServerTLSSecret
    }
    if certs.ReplicationTLSSecret != "" {
        plan.ClientTLSSecret = certs.ReplicationTLSSecret
    }
}
```

### cluster_pki.go — Independent per-field generation

Replace `hasUserCertificates()` with per-field checks:

| Field set by user | Operator skips generating |
|---|---|
| `ServerCASecret` | SelfSigned Issuer + CA Certificate |
| `ClientCASecret` | (currently same as ServerCA, no separate resource) |
| `ServerTLSSecret` | Per-instance Server Certificates |
| `ReplicationTLSSecret` | Operator Client Certificate |

When `ServerTLSSecret` is set:
- Per-instance server certificates are NOT created
- `plan.UserServerTLSSecret` is populated (already works)
- The CA Issuer is still needed if the user didn't also provide `ServerCASecret`

When `ServerCASecret` is set:
- The operator skips creating the SelfSigned Issuer, CA Certificate, and CA
  Issuer
- The user-provided secret must already be a valid CA (has `ca.crt` key)
- The cert-manager CA Issuer references the user's secret

When `ReplicationTLSSecret` is set:
- The operator skips creating the operator client Certificate
- `plan.ClientTLSSecret` is populated (already works)

### ServerAltDNSNames wiring

In `ensureCertificates()`, pass `certs.ServerAltDNSNames` to
`serverDNSNames(cluster, instName, altNames...)`. Append them after the
auto-generated SANs so they are included in the cert-manager Certificate spec.

### User-secret validation

Add `validateUserCertificates(ctx, cluster)` called before cert-manager
resource creation. For each user-provided secret reference:

1. **Secret exists**: `r.Get()` succeeds.
2. **Secret type**: `kubernetes.io/tls` for server/client certs; `kubernetes.io/tls`
   or `Opaque` for CA secrets.
3. **Required keys**: `tls.crt` + `tls.key` for cert secrets; `ca.crt` for CA
   secrets.
4. **PEM validity**: `tls.X509KeyPair()` parses cert secrets; `x509.ParseCertificate()`
   parses CA bundles. (Best-effort, not blocking.)
5. **CA check for CA secrets**: If `ServerCASecret` or `ClientCASecret` is set,
   verify the secret actually contains a CA certificate (`IsCA: true` or
   `BasicConstraintsValid`).

Failures produce a `Blocked` condition with a clear message (e.g. "User-provided
server TLS secret 'my-certs' is missing key 'tls.key'").

### CertificatesStatus population

After reconciliation, populate `status.certificates` with:
- `ServerCASecret` = resolved `plan.ServerCASecretName`
- `ServerTLSSecret` = resolved `plan.UserServerTLSSecret` or the generated name
- `ClientCASecret` = resolved `plan.ClientCASecretName`
- `ReplicationTLSSecret` = resolved `plan.ClientTLSSecret`
- `Expirations`: Read cert-manager Certificate status for each managed cert and
  extract `status.notAfter`.

### status_client.go — Separate CA for mTLS

The status client currently uses `conn.CASecretName` (defaulting to
`{cluster}-ca`) for the client-ca verification. When `ClientCASecret` is set,
use that value instead. No change needed for the server auth (the status client
connects to the server, verifying the server cert against the server CA).

### cluster_pod.go — Separate client-ca volume

Currently the `client-ca` volume uses `plan.CASecretName`. Change to
`plan.ClientCASecretName` so user overrides of `ClientCASecret` correctly
flow into the Pod spec.

## Decision: Same-CA vs Separate-CA default

In CNPG, server CA and client CA are independently configurable and have
separate generated secrets. In cnmsql, the simpler model uses one CA for
both server and client certs (generated by cert-manager). For M11:

- **Keep the single-CA default**: Both `ServerCASecretName` and
  `ClientCASecretName` default to `{cluster}-ca`.
- **Allow independent overrides**: User can set `ServerCASecret` to a different
  secret than `ClientCASecret`.
- This is backward-compatible (no change to existing clusters) and allows the
  full CNPG flexibility when users opt in.

## Implementation order

1. Split `CASecretName` into `ServerCASecretName` / `ClientCASecretName` in
   `clusterPlan` and update all references.
2. Fix `buildPlan` certs block to not overwrite.
3. Update `clusterPod` `client-ca` volume to use `plan.ClientCASecretName`.
4. Rework `ensureCertificates()` to check per-field overrides instead of
   all-or-nothing `hasUserCertificates()`.
5. Wire `ServerAltDNSNames` into `serverDNSNames()`.
6. Add `validateUserCertificates()` in a new `cluster_cert_validation.go` file.
7. Update `CertificatesStatus` type to inline configuration.
8. Populate `status.certificates` after reconciliation.
9. Run `make generate manifests` (CertificatesStatus change).
10. Unit tests: independent override paths, separate CA fields, alt DNS names,
    secret validation (missing, wrong type, bad PEM, missing CA), status
    population.
11. `make lint-fix && make test`.
12. Kind e2e: Create a cluster with `certificates.serverTLSSecret` pointing at a
    user-provided TLS secret; assert the operator skips server cert generation
    but still creates CA/client certs. Full override (all four secrets) skips all
    cert-manager resources. `ServerAltDNSNames` appear in cert-manager
    Certificate.
13. Docs: update `security-model.md` with user-TLS configuration examples,
    update `cluster-lifecycle.md` YAML examples.

## Testing

### Unit
- `buildPlan`: `ServerCASecretName` / `ClientCASecretName` default to
  `{cluster}-ca`; user overrides are independent.
- `ensureCertificates`: `ServerCASecret` set → skip SelfSigned+CA+CAIssuer but
  still create server certs; `ServerTLSSecret` set → skip per-instance server
  certs; all four set → skip everything; partial sets work.
- `validateUserCertificates`: missing secret → error; wrong type → error;
  missing keys → error; expired cert → warning (not blocking); valid → ok.
- `serverDNSNames`: alt names appended after auto-generated SANs.
- `CertificatesStatus` inline serialization.

### E2E
- Partial override: user provides `ServerTLSSecret`, operator still generates
  CA + client certs. Cluster becomes Ready, Pods mount the user server cert
  and the operator-generated CA.
- Full override: user provides all four secrets. No cert-manager resources
  created. Cluster becomes Ready.
- `ServerAltDNSNames`: additional SANs in the generated cert-manager Certificate.

## Acceptance criteria

- Each of the four `CertificatesConfiguration` fields can be set independently;
  the operator generates cert-manager resources only for unset fields.
- `ServerCASecret` and `ClientCASecret` overrides work independently.
- `ServerAltDNSNames` are included in generated server certificate SANs.
- User-provided secrets are validated (existence, type, keys, basic PEM) before
  use; invalid secrets produce a clear `Blocked` condition.
- `CertificatesStatus` inlines the configuration and reports expirations.
- Existing clusters with no `certificates` spec continue working unchanged
  (full operator-generated cert-manager path).
- `make generate manifests`, `make lint-fix`, `make test`, and the M11 Kind e2e
  pass.
- Docusaurus docs updated and `npm run build` passes.
