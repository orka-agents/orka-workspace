set -eu
for i in 1 2 3; do
  name=pod-baseline-$i
  start=$(date +%s%N)
  kubectl -n tenant-a run $name --image=orka-fiber-home:spike --image-pull-policy=Never --restart=Never \
    --env=ORKA_ACP_CREDENTIAL_BOOTSTRAP_NONCE=$(openssl rand -hex 32) \
    --env=ORKA_ACP_CREDENTIAL_BOOTSTRAP_PUBLIC_KEY=11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo \
    --env=ORKA_ACP_LISTEN_ADDRESS=:8080 --env=ORKA_ACP_PROVIDER=codex --env=HOME=/tmp \
    --command -- /usr/share/orka-fiber/rootfs/usr/local/bin/orka-acp-runtime >/dev/null
  until ip=$(kubectl -n tenant-a get pod $name -o jsonpath='{.status.podIP}') && [ -n "$ip" ] && kubectl -n tenant-a exec spike-check -- curl -sf -m 1 http://$ip:8080/v2/health >/dev/null 2>&1; do :; done
  echo "$name create->health $(( ($(date +%s%N) - start) / 1000000 ))ms"
  kubectl -n tenant-a delete pod $name --wait=false >/dev/null
done
