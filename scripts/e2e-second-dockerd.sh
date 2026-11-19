#!/usr/bin/env bash
# e2e-second-dockerd.sh — run a second Docker daemon for the black-box suite.
#
# A docker-backend needs a Docker daemon to itself, as it has in production:
# it refuses to work beside another backend's containers. The two-backend
# tests in test/e2e/chain therefore need two daemons. This starts the second
# as a transient systemd unit with its own socket, data and runtime state.
#
# Usage (as root):
#   sudo scripts/e2e-second-dockerd.sh start   # prints the socket path
#   sudo scripts/e2e-second-dockerd.sh stop    # stops it and deletes its data
#
# Then run the suite with both sockets:
#   FRED_E2E_DOCKER_SOCKETS=/var/run/docker.sock,/run/fred-e2e-docker-2/docker.sock make test-e2e
set -euo pipefail

name=fred-e2e-docker-2
run_dir=/run/$name
data_dir=/var/lib/$name
socket=$run_dir/docker.sock

[[ $EUID -eq 0 ]] || { echo "run as root" >&2; exit 1; }

case "${1:-}" in
  start)
    if systemctl is-active --quiet "$name"; then
      echo "$socket"
      exit 0
    fi
    install -d -m 0755 "$run_dir"
    install -d -m 0710 "$data_dir"
    # Only the settings that decide how images are stored and fetched and how
    # containers run are taken from the system daemon. Everything else there
    # names a place, a network or a listener (its metrics address, for one)
    # that the system daemon already holds.
    if [[ -s /etc/docker/daemon.json ]]; then
      jq 'with_entries(select(.key | IN(
            "features", "storage-driver", "storage-opts",
            "exec-opts", "cgroup-parent", "default-runtime", "runtimes",
            "registry-mirrors", "insecure-registries", "dns", "proxies")))' \
        /etc/docker/daemon.json >"$run_dir/daemon.json"
    else
      echo '{}' >"$run_dir/daemon.json"
    fi
    # - No default bridge and no firewall rules: the system daemon owns
    #   docker0 and the DOCKER chains, and two daemons would fight over them.
    #   Lease networks still connect their own containers.
    # - Its own address pool, so its lease networks never take a subnet the
    #   system daemon uses.
    # - Its own containerd namespaces, because dockerd uses the system
    #   containerd when one runs.
    systemd-run --quiet --collect --unit "$name" \
      dockerd \
      --config-file "$run_dir/daemon.json" \
      --host "unix://$socket" \
      --data-root "$data_dir" \
      --exec-root "$run_dir/exec" \
      --pidfile "$run_dir/docker.pid" \
      --bridge none \
      --iptables=false \
      --ip6tables=false \
      --default-address-pool base=10.213.0.0/16,size=24 \
      --containerd-namespace "$name" \
      --containerd-plugins-namespace "$name-plugins"
    for _ in $(seq 1 60); do
      if docker --host "unix://$socket" info >/dev/null 2>&1; then
        echo "$socket"
        exit 0
      fi
      sleep 1
    done
    echo "the second Docker daemon did not answer within 60 seconds:" >&2
    journalctl --no-pager -u "$name" -n 50 >&2 || true
    exit 1
    ;;
  stop)
    # The data is deleted only once the daemon is known to have stopped. If
    # systemd cannot be asked, or does not stop the unit, everything stays.
    state="$(systemctl show --property ActiveState --value "$name")"
    if [[ $state != inactive && $state != failed ]]; then
      systemctl stop "$name"
      state="$(systemctl show --property ActiveState --value "$name")"
    fi
    if [[ $state != inactive && $state != failed ]]; then
      echo "$name is still $state; its data was kept" >&2
      exit 1
    fi
    if [[ -s $run_dir/docker.pid ]] && kill -0 "$(<"$run_dir/docker.pid")" 2>/dev/null; then
      echo "dockerd (pid $(<"$run_dir/docker.pid")) is still running; its data was kept" >&2
      exit 1
    fi
    rm -rf -- "$data_dir" "$run_dir"
    ;;
  *)
    echo "usage: $0 start|stop" >&2
    exit 2
    ;;
esac
