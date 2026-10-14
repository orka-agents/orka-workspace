cg=$(dirname "$(cut -d: -f3 /proc/1/cgroup)"); root=/sys/fs/cgroup$cg
echo "home=$(( $(cat $root/memory.current) / 1048576 ))MiB grants=$(( $(cat $root/grants/memory.current) / 1048576 ))MiB"
for d in "$root"/grants/*/; do [ -f "$d/memory.current" ] || continue
  printf '  %s %sMiB:' "$(basename "$d")" $(( $(cat "$d/memory.current") / 1048576 ))
  for l in "$d"*/; do [ -f "$l/memory.current" ] && printf ' %s=%sMiB' "$(basename "$l")" $(( $(cat "$l/memory.current") / 1048576 )); done; echo
done
