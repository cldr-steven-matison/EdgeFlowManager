# Chapter 10: MiNiFi C++ to NiFi over Secure Site-to-Site

Site-to-Site (S2S) is how FlowFiles move between a MiNiFi agent and NiFi. This chapter covers the first local leg: a **MiNiFi C++** agent on Kubernetes sending FlowFiles into a CFM-operator-managed NiFi over secure S2S — mTLS, HTTP transport, riding the existing HTTPS port. The Java leg (Chapter 11) reuses this same NiFi-side setup; only the source agent changes.

The one idea that shapes everything here: on a CFM-operator NiFi you don't POST authorization policies, you **declare** them. The operator owns the authorizer.

## The shape

```
MiNiFi C++ agent  ──secure S2S (mTLS, HTTP transport, :8443)──►  NiFi:  from-minifi (input port) → funnel
GenerateFlowFile → RemoteProcessGroup                                   peer authorized by a User CR
```

Three moving parts: a NiFi **input port** to receive, a **peer authorization** for the agent's identity, and the agent's **RemoteProcessGroup** pointing at NiFi's web URL.

## Transport: HTTP, not RAW

NiFi S2S offers two transports: RAW (a dedicated socket on `nifi.remote.input.socket.port`) and HTTP (S2S tunnelled over the existing HTTPS port). RAW needs its own exposed socket; HTTP rides the `8443` path NiFi already serves. Use **HTTP** — set `nifi.remote.input.http.enabled=true` and the RPG's target is just the NiFi web URL.

## NiFi side — the S2S target

Enable S2S input on the NiFi CR (`configOverride.nifiProperties.upsert`):

```yaml
nifi.remote.input.host: nifi-0.nifi.<ns>.svc.cluster.local
nifi.remote.input.secure: "true"
nifi.remote.input.http.enabled: "true"
```

The operator rolls the pod to apply. Then create the receive target on the canvas: an **Input Port** named `from-minifi` with a downstream **funnel** and a connection — an input port with no outgoing connection is invalid and won't start. Set it RUNNING and note its UUID; the peer policy names it.

Authorization is declarative. The operator reconciles users and access policies from `User` / `UserGroup` / `AccessPolicyProfile` CRs — hand-POSTing a policy as the seeded admin returns `500 Unable to save Authorizations`, because you're writing to a store the operator manages. Declare the peer instead:

```yaml
apiVersion: cfm.cloudera.com/v1alpha1
kind: User
metadata: { name: minifi-s2s, namespace: <ns> }
spec:
  identity: "minifi-s2s"                 # the cert's MAPPED identity (SAN), not its DN
  instanceTarget: { kind: Nifi, name: <nifi>, namespace: <ns> }
  accessPolicies:
    - { actions: [write], resources: [/data-transfer/input-ports/<from-minifi-uuid>] }
    - { actions: [read],  resources: [/site-to-site] }
```

The operator writes those policies into NiFi as the true policy owner — the exact grant a REST POST can't persist.

## Source side — the C++ RPG

Build the agent flow through the EFM Flow Designer (the agent is EFM-managed): `GenerateFlowFile → RemoteProcessGroup → from-minifi`. The RPG targets the NiFi web URL over HTTPS with HTTP transport:

- `targetUris = https://nifi-web.<ns>.svc.cluster.local:8443`
- `transportProtocol = HTTP`
- the connection's destination is a `REMOTE_INPUT_PORT` whose id is the `from-minifi` UUID.

Validate the flow (`/validate` returns `[]`) before publishing.

> Hand-authoring the C++ `config.yml` instead of using the Designer? The strict-YAML parser needs an explicit UUID `id` on every component, and the `Remote Processing Groups:` key must be present even when empty. Upstream examples spell it inconsistently (`Remote Process Groups` vs `Remote Processing Groups`) — pin the exact key against your agent version.

## Client SSL lives in `minifi.properties`, not the flow

This is the C++-specific catch: a MiNiFi **C++** RemoteProcessGroup has **no SSL-context-service field**. Secure S2S uses the agent's global client identity instead:

```properties
nifi.remote.input.secure=true
nifi.security.client.certificate=/path/to/minifi-s2s.crt
nifi.security.client.private.key=/path/to/minifi-s2s.key
nifi.security.client.ca.certificate=/path/to/ca.crt
```

Mount the agent's cert (SAN `minifi-s2s`, signed by NiFi's CA) and set those keys — bake them into the boot script so a restart keeps them. The cert's SAN must match the `User.spec.identity`; NiFi maps identity by SAN, not subject DN.

## Verify

Send a FlowFile and confirm it arrives. The agent log shows the transaction:

```
[SiteToSiteClient] Site to Site transaction <uuid> sent flow 1 flow records, with total size 32
[SiteToSiteClient] Site2Site transaction <uuid> peer finished transaction
```

On the NiFi side, the `from-minifi → funnel` queue climbs one FlowFile every few seconds.

![The from-minifi input port receiving MiNiFi C++ FlowFiles via secure Site-to-Site, queued into a downstream funnel](../images/minifi-s2s-from-minifi-queue.png)

## What not to do

- **Don't POST authorization policies to a CFM-operator NiFi.** Declare them with `User`/`UserGroup`/`AccessPolicyProfile` CRs. A `403 No applicable policies` / `500 Unable to save Authorizations` is the operator telling you to declare, not POST.
- **Don't set an identity by its subject DN.** Identity maps by cert **SAN**. Give the peer cert `SAN: DNS:minifi-s2s` and set `User.spec.identity: minifi-s2s`, or the grant lands on a string nobody authenticates as.
- **Don't reference the port policy before the port exists.** `/data-transfer/input-ports/<id>` is per-port; create `from-minifi` first, then declare the peer User with its real UUID.
- **Don't hand-scale the operator's NiFi StatefulSet.** Scaling `sts/nifi` to 0 deadlocks the operator's scale-up state machine (`NoViableLeaders`). For PVC surgery, pause the operator and use a debug pod; recover a wedged state by delete + re-apply of the Nifi CR (the PVCs survive).
- **Don't let the EFM deployer's own minifi hold the lock.** The deployer starts a minifi during install; a second `exec` dies on `Could not acquire LOCK`. In the boot script, `pkill` the deployer's instance and remove the stale `LOCK` before setting the security props and `exec`ing.
