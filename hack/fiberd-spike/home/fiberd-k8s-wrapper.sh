#!/bin/sh
# Probes run `fiberd-k8s -healthz`; only the serving process starts the proxy.
for arg in "$@"; do
  [ "$arg" = "-healthz" ] && exec /usr/local/bin/fiberd-k8s "$@"
done
allow="example.com:443,*.svc:*,*.svc.cluster.local:*"
[ -r /etc/orka-fiber/egress-allow ] && allow="$(cat /etc/orka-fiber/egress-allow)"
/usr/local/bin/orka-fiber-egress -run-dir /var/lib/fiberd/run -allow "$allow" &
exec /usr/local/bin/fiberd-k8s "$@"
