# Chapter 11: MiNiFi Java to NiFi over Secure Site-to-Site

This chapter runs a MiNiFi **Java** agent that streams FlowFiles into a CFM-operator NiFi over secure Site-to-Site (S2S) on Kubernetes — the Java counterpart to the C++ Site-to-Site path. The flow is deliberately trivial (`GenerateFlowFile → Remote Process Group → NiFi input port`); the whole chapter is really about the one configuration detail that decides whether the mTLS handshake completes, because MiNiFi Java stores that detail somewhere non-obvious.

The NiFi side is identical to the C++ path — the same input port, the same declarative `User` authorization. What changes is the source agent, and MiNiFi Java's client-SSL wiring is a different animal from C++.

---

## The shape

```
GenerateFlowFile ──success──▶ Remote Process Group (HTTP, 8443) ──▶ from-minifi (NiFi input port)
```

`GenerateFlowFile` produces a 32-byte test payload every 5 seconds. The Remote Process Group (RPG) targets NiFi's web URL and transmits to a remote input port named `from-minifi`. Everything crosses the wire as mutually-authenticated TLS.

---

## Transport: HTTP, not RAW

NiFi S2S offers two transports: **RAW** (a dedicated socket on `nifi.remote.input.socket.port`) and **HTTP** (S2S tunnelled over the existing HTTPS port). RAW needs its own exposed port on top of NiFi's pod-IP binding — more plumbing, and that socket isn't exposed by the operator. Use **HTTP**: it rides the same `8443` the NiFi API already listens on, so the RPG's target URL is just the NiFi web URL and the SSL context handles the rest.

```
targetUris        = https://nifi-web.cfm-streaming.svc.cluster.local:8443
transportProtocol = HTTP
```

---

## NiFi side — the S2S target

The NiFi node runs in namespace `cfm-streaming` behind the `nifi-web` service (`8443` HTTPS). Three things have to be true before an agent can transmit:

1. **S2S input is enabled.** Patch the `Nifi` CR's `configOverride.nifiProperties.upsert`:

   ```yaml
   nifi.remote.input.host: nifi-web.cfm-streaming.svc.cluster.local
   nifi.remote.input.secure: "true"
   nifi.remote.input.http.enabled: "true"
   ```

2. **An input port exists and is RUNNING.** On the root canvas, add an input port `from-minifi`, wire it to a downstream funnel (so received FlowFiles have somewhere to land), and start it. Note its UUID — the RPG addresses the port by ID.

3. **The agent's identity is authorized — declaratively.** On a CFM-operator NiFi you don't `POST` the access policy, you declare it. A `User` CR whose identity matches the agent's client-cert SAN, granted `write` on `/data-transfer/input-ports/<from-minifi-uuid>` and `read` on `/site-to-site`, and the operator reconciles the policy for you.

> **⚠️ The `nifi-web` service is not created for you.** The operator only creates the headless `nifi` service. The `nifi-web` service on `8443` must be created by hand, or every operator reconcile against `https://nifi-web…:8443/nifi-api/…` fails with `no such host` and the users/policies never land. That hostname is in the node cert's SAN, so TLS validates as soon as the service exists.

The client certificate itself is a cert-manager `Certificate` signed by the operator's CA, with **SAN = the agent identity** (identity maps by SAN, not DN). That produces a keystore/truststore pair the agent mounts.

---

## Source side — the RPG

The MiNiFi Java flow is one processor, one RPG, one connection:

- `GenerateFlowFile` — `File Size` 32 B, `Batch Size` 1, `Run Schedule` 5 sec.
- A **Remote Process Group** with `targetUris` = the NiFi web URL and `transportProtocol` = `HTTP`.
- A connection from `GenerateFlowFile`'s `success` relationship to the RPG's remote input port, addressed by the `from-minifi` UUID.

The C++ agent's strict-YAML quirks (every component needs an explicit UUID, `Remote Processing Groups: []` must be present even when empty) do **not** apply here — MiNiFi Java carries its flow as a `flow.json.gz` / `flow.json.raw` pair, not YAML.

---

## The configuration that actually matters — client SSL lives in `bootstrap.conf`

This is the whole chapter. Get the flow perfect and the handshake still fails like this:

```
o.a.n.r.client.PeerSelector Unable to refresh remote group peers due to:
  (certificate_unknown) PKIX path building failed:
  unable to find valid certification path to requested target
```

**Symptom.** The RPG's first S2S call throws `PKIX path building failed` on the **client** side — MiNiFi validating NiFi's *server* certificate and failing to build a trust path to it. Not "the server rejected my client cert"; the client can't even trust the server.

**Diagnosis.** The keystore and truststore are correct (client identity signed by the operator CA, truststore holding that same CA, server cert signed by it). The S2S client just isn't loading them — it's falling back to the JVM's default `cacerts`, which has never heard of a private CA. The reason is a MiNiFi Java trait that trips everyone once: **MiNiFi Java regenerates `conf/minifi.properties` from `conf/bootstrap.conf` on every start.** Edit `minifi.properties` directly and your change is gone on the next restart. The stock `bootstrap.conf` ships the S2S SSL keys **empty** and `use.parent.ssl=false`, so the regenerated `minifi.properties` has no client SSL context.

**Fix.** Set the client SSL config in `bootstrap.conf` — the source of truth the regeneration reads — and turn on parent SSL so the RPG uses that framework context:

```properties
nifi.minifi.security.keystore=/certs-ks/keystore.p12
nifi.minifi.security.keystoreType=PKCS12
nifi.minifi.security.keystorePasswd=changeit
nifi.minifi.security.keyPasswd=changeit
nifi.minifi.security.truststore=/opt/minifi/conf/truststore-ks.p12
nifi.minifi.security.truststoreType=PKCS12
nifi.minifi.security.truststorePasswd=changeit
# Ignore per-processor SSL controller services and use the parent MiNiFi SSL context
nifi.minifi.flow.use.parent.ssl=true
```

Restart, and the regenerated `minifi.properties` now carries `nifi.security.*` plus `use.parent.ssl=true`. The RPG picks up the framework SSL context, the handshake completes, and FlowFiles transit — zero PKIX errors.

---

## Running it unmanaged — a self-contained image

There's no published `minifi-java` container image, and running the agent under the EFM deployer has a catch: the deployer rewrites `bootstrap.conf` on every **pod** boot, so the SSL fix above survives a MiNiFi-process restart but not a pod restart. The durable answer is a small image that bakes the fixed config and runs the agent directly, with no deployer in the boot path.

```dockerfile
FROM eclipse-temurin:21-jre

# Auto-extracts to /minifi-<version>
ADD minifi.tar.gz /

# Fixed config + flow + CA truststore, over the stock conf
COPY bootstrap.conf    /minifi-<version>/conf/bootstrap.conf
COPY flow.json.raw     /minifi-<version>/conf/flow.json.raw
COPY flow.json.gz      /minifi-<version>/conf/flow.json.gz
COPY flow-identifier   /minifi-<version>/conf/flow-identifier
COPY truststore-ks.p12 /minifi-<version>/conf/truststore-ks.p12

WORKDIR /minifi-<version>
ENTRYPOINT ["bin/minifi.sh", "run"]   # foreground
```

Two things separate a working image from a crashing one:

- **`flow.json.raw` is authoritative in MiNiFi Java.** `flow.json.gz` is derived from it. Bake only the `.gz` and MiNiFi regenerates an empty default flow, recompresses over your `.gz`, and starts **zero** processors — the RPG never even loads. Bake `flow.json.raw` (and `flow-identifier`) too.
- **The client keystore holds a private key — don't bake it into the image.** Mount it at runtime from the Kubernetes secret and point `bootstrap.conf` at the mount (`/certs-ks/keystore.p12`). The CA-only truststore is public and safe to bake.

Set `c2.enable=false` in the baked `bootstrap.conf` — with the flow already on disk there's no reason to phone an EFM home. Run it as a plain Deployment mounting the keystore secret:

```yaml
volumeMounts:
  - { name: s2s-ks, mountPath: /certs-ks, readOnly: true }
volumes:
  - name: s2s-ks
    secret: { secretName: minifi-s2s-keystore }
```

---

## Verify

Send a FlowFile and confirm it lands. Agent side — the RPG refreshes peers (that's the handshake) and reports each send:

```bash
POD=$(kubectl -n cld-streaming get pod -l app=minifi-java-unmanaged -o jsonpath='{.items[0].metadata.name}')
kubectl -n cld-streaming exec "$POD" -- \
  grep -E "Successfully refreshed Flow Contents|Successfully sent" \
  /minifi-<version>/logs/minifi-app.log | tail
```

```
o.a.n.remote.StandardRemoteProcessGroup Successfully refreshed Flow Contents for
  RemoteProcessGroup[https://nifi-web.cfm-streaming.svc.cluster.local:8443]
o.a.nifi.remote.StandardRemoteGroupPort RemoteGroupPort[name=from-minifi,...]
  Successfully sent [StandardFlowFileRecord[...]] (32 bytes) to .../nifi-api in 14 milliseconds
```

NiFi side — the receive count climbs and the FlowFiles queue on the funnel behind the input port:

```bash
curl -s --cert /certs/tls.crt --key /certs/tls.key --cacert /certs/ca.crt \
  "https://nifi-web.cfm-streaming.svc.cluster.local:8443/nifi-api/flow/process-groups/root/status?recursive=true" \
  | grep -oE '"(flowFilesReceived|queued)":("[^"]*"|[0-9]+)'
```

```
"flowFilesReceived":176
"queued":"176 (5.5 KB)"
```

S2S is transactional, so an agent-side "Successfully sent" already means NiFi received and committed the FlowFile — the receive count is confirmation, not the proof itself.

---

## What not to do

- **Don't edit `minifi.properties` to fix client SSL.** MiNiFi Java rewrites it from `bootstrap.conf` on every start; your edit lasts exactly one run. Set `nifi.minifi.security.*` and `nifi.minifi.flow.use.parent.ssl` in `bootstrap.conf`.
- **Don't bake only `flow.json.gz`.** Without `flow.json.raw`, MiNiFi generates an empty default flow and starts 0 processors. Bake both plus `flow-identifier`.
- **Don't bake the client keystore into the image.** It carries a private key. Mount it from a secret; bake only the public CA truststore.
- **Don't read `PKIX path building failed` as "the server rejected my client cert."** When the stack is client-side (`PKIXValidator` while consuming the handshake), it's *your* truststore that isn't loaded — chase the SSL context wiring, not the authorization policy.
- **Don't redeploy NiFi mid-transfer.** Restarting the target NiFi during an active S2S transfer drops in-flight FlowFiles with `unexpected end of stream`. Drain transfers before any redeploy.
