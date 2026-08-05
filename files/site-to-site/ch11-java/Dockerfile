# Unmanaged MiNiFi Java agent — secure Site-to-Site into CFM-operator NiFi.
# Resolves the Ch11 blocker (#98) and the missing minifi-java image (#35).
#
# Why unmanaged: the EFM deployer regenerates minifi.properties from bootstrap.conf
# on every start AND rewrites bootstrap.conf itself on every *pod* boot, wiping the
# nifi.minifi.security.* / use.parent.ssl config the S2S client needs. Baking a fixed
# bootstrap.conf into the image and running with c2.enable=false removes both the
# deployer bootstrap and the C2 channel, so the client keystore/truststore stick and
# the mTLS handshake completes.
#
# Build context artifacts (all pulled from the proven live agent):
#   minifi.tar.gz      - CEM MiNiFi Java 2.24.08.0-19 binary (NOT committed; from
#                        ~/efm-binaries/staging/binaries/java/linux/2.24.08.0-19/)
#   bootstrap.conf     - fixed: nifi.minifi.security.* -> mounted keystore + baked
#                        truststore, nifi.minifi.flow.use.parent.ssl=true, c2.enable=false
#   flow.json.gz       - published flow: GenerateFlowFile -> RPG(HTTP 8443) -> from-minifi
#   truststore-ks.p12  - PKCS12 truststore holding ONLY the public cfm-operator-ca cert
#
# The client keystore.p12 (private key) is NOT baked — it is mounted at runtime from
# the minifi-s2s-keystore secret at /certs-ks (see minifi-java-unmanaged.yaml).
FROM eclipse-temurin:21-jre

# Auto-extracts to /minifi-2.24.08.0-19 (matches the truststore path in bootstrap.conf)
ADD minifi.tar.gz /

# Override the tarball's stock conf with the proven S2S config + flow + CA truststore
COPY bootstrap.conf    /minifi-2.24.08.0-19/conf/bootstrap.conf
# flow.json.raw is authoritative in MiNiFi Java 2.x; .gz is derived from it. Bake
# both plus the flow-identifier, else minifi regenerates an empty default flow.
COPY flow.json.raw     /minifi-2.24.08.0-19/conf/flow.json.raw
COPY flow.json.gz      /minifi-2.24.08.0-19/conf/flow.json.gz
COPY flow-identifier   /minifi-2.24.08.0-19/conf/flow-identifier
COPY truststore-ks.p12 /minifi-2.24.08.0-19/conf/truststore-ks.p12

WORKDIR /minifi-2.24.08.0-19
# Foreground; minifi regenerates minifi.properties from bootstrap.conf on start.
ENTRYPOINT ["bin/minifi.sh", "run"]
