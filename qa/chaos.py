#!/usr/bin/env python3
"""Freightdock's failure tests, on a real cluster: what a partition or a dead manager does to it, measured.

    python3 qa/chaos.py <cluster.json> [scenario ...]      (no scenario: all of them, in order)

It reaches every node over ssh (the command in the config, `ssh` by default; give it keys or an agent) and cuts only
the managers' ports (7450-7452, nftables, a table of its own named `fdchaos`) or stops a manager's service. Every cut
also schedules its own undoing on the node (systemd-run, `heal_s` later), so an interrupted run never leaves a node
cut off. It never removes anything else, and gives the probe node its labels back at the end.

While a scenario runs, a watcher lists the containers of every worker each second: a workload with one replica
(`singletons`, by container name) seen on two nodes at once is a split brain, the one thing that must never happen,
and fails the run whatever else passed.

The config (kept out of the repository, it names your hosts):

    {
      "ssh": ["ssh"],
      "managers": [{"host": "root@192.0.2.11"}, {"host": "root@192.0.2.12"}, {"host": "root@192.0.2.13"}],
      "workers":  [{"host": "root@192.0.2.21"}, {"host": "admin@192.0.2.22", "sudo": true}],
      "probe_node": "<a node id whose labels the probe may change for a while>",
      "worker_cut": {"host": "root@192.0.2.21", "node": "<its node id>",
                     "power_on": {"host": "root@192.0.2.5", "cmd": "pct start 113"}},
      "singletons": ["nfs-writer-1"],
      "heal_s": 600
    }

`sudo: true` runs the host's commands through `sudo -n`, or with SUDO_PASSWORD from the environment piped to `sudo -S`.
"""

import json
import os
import shlex
import subprocess
import sys
import threading
import time

DATA = "/var/lib/freightdock-manager"
PORTS = "7450-7452"


class Host:
    def __init__(self, ssh, spec):
        self.name = spec["host"]
        self.sudo = spec.get("sudo", False)
        self.ssh = list(ssh) + ["-o", "BatchMode=no", "-o", "ConnectTimeout=5", "-o", "ServerAliveInterval=3", "-o", "ServerAliveCountMax=2", self.name]

    def ip(self):
        return self.name.split("@")[-1]

    def run(self, cmd, stdin=None, timeout=20):
        """Runs `cmd` (sh) on the host, as root; gives back (exit code, stdout). A host out of reach is (255, '')."""
        if self.sudo:
            pw = os.environ.get("SUDO_PASSWORD")
            wrapped = f"sudo -S -p '' sh -c {shlex.quote(cmd)}" if pw else f"sudo -n sh -c {shlex.quote(cmd)}"
            if pw:
                stdin = pw + "\n" + (stdin or "")
        else:
            wrapped = cmd
        try:
            p = subprocess.run(self.ssh + [wrapped], input=stdin, capture_output=True, text=True, timeout=timeout)
            return p.returncode, p.stdout
        except subprocess.TimeoutExpired:
            return 255, ""


class Manager(Host):
    def api(self, method, path, body=None, timeout=12):
        """The manager's admin API on the host itself (its token never leaves it): (HTTP status, JSON or None)."""
        data = "" if body is None else json.dumps(body)
        cmd = (f"curl -s -m {timeout - 2} -o - -w '\\n%{{http_code}}' -X {method} -H 'content-type: application/json' "
               f"-H \"Authorization: Bearer $(cat {DATA}/admin.token)\" --data-binary @- http://127.0.0.1:7451{path}")
        code, out = self.run(cmd, stdin=data, timeout=timeout)
        if code != 0 or not out.strip():
            return 0, None
        text, _, status = out.rpartition("\n")
        try:
            return int(status), (json.loads(text) if text.strip() else None)
        except ValueError:
            return int(status) if status.isdigit() else 0, None

    def cluster(self):
        s, v = self.api("GET", "/v1/cluster", timeout=6)
        return v if s == 200 and isinstance(v, dict) else None

    def leader_addr(self):
        c = self.cluster()
        if not c or not c.get("quorate"):
            return None
        return next((r["addr"] for r in c.get("raft", []) if r.get("leader")), None)


class Report:
    def __init__(self):
        self.rows = []
        self.failed = False

    def check(self, what, ok, detail=""):
        self.rows.append((ok, what, detail))
        self.failed |= not ok
        print(f"  {'PASS' if ok else 'FAIL'}  {what}{('  (' + detail + ')') if detail else ''}", flush=True)

    def note(self, what):
        print(f"        {what}", flush=True)


class Watcher(threading.Thread):
    """Lists every worker's containers each second; remembers any singleton seen running on two nodes at once."""

    def __init__(self, workers, singletons):
        super().__init__(daemon=True)
        self.workers, self.singletons = workers, set(singletons)
        self.stop_at = None
        self.split = []
        self.seen = {}

    def run(self):
        while self.stop_at is None:
            where = {}
            threads = []
            out = {}

            def look(w):
                code, o = w.run("docker ps --format '{{.Names}}'", timeout=8)
                out[w.name] = o.split() if code == 0 else None

            for w in self.workers:
                t = threading.Thread(target=look, args=(w,))
                t.start()
                threads.append(t)
            for t in threads:
                t.join()
            for host, names in out.items():
                for n in names or []:
                    if n in self.singletons:
                        where.setdefault(n, []).append(host)
            now = time.strftime("%H:%M:%S")
            for n, hosts in where.items():
                if len(hosts) > 1:
                    self.split.append((now, n, hosts))
            self.seen = where
            time.sleep(1)

    def finish(self):
        self.stop_at = time.time()
        self.join(timeout=15)


def wait_for(what, fn, limit_s, every=1.0):
    """Polls `fn` until it gives something true, for at most `limit_s`: (value, seconds taken), or (None, limit)."""
    t0 = time.time()
    while time.time() - t0 < limit_s:
        v = fn()
        if v:
            return v, time.time() - t0
        time.sleep(every)
    return None, limit_s


def cut(host, peers, heal_s):
    """Drops the managers' ports between `host` and `peers` (both ways), undone by itself after `heal_s`."""
    ins = "".join(f"ip saddr {p} tcp dport {{ {PORTS} }} drop\nip saddr {p} tcp sport {{ {PORTS} }} drop\n" for p in peers)
    outs = "".join(f"ip daddr {p} tcp dport {{ {PORTS} }} drop\nip daddr {p} tcp sport {{ {PORTS} }} drop\n" for p in peers)
    ruleset = (f"table inet fdchaos {{\nchain in {{\ntype filter hook input priority -10;\n{ins}}}\n"
               f"chain out {{\ntype filter hook output priority -10;\n{outs}}}\n}}\n")
    script = ("nft delete table inet fdchaos 2>/dev/null; nft -f - && "
              f"systemd-run --quiet --on-active={int(heal_s)} nft delete table inet fdchaos")
    code, _ = host.run(script, stdin=ruleset)
    return code == 0


def heal(host):
    host.run("nft delete table inet fdchaos 2>/dev/null; true")


def probe_write(m, node, base, value):
    labels = dict(base)
    labels["chaos"] = value
    s, _ = m.api("PUT", f"/v1/nodes/{node}/labels", {"labels": labels}, timeout=12)
    return s


def probe_read(m, node):
    s, v = m.api("GET", f"/v1/nodes/{node}", timeout=6)
    return (v or {}).get("labels", {}).get("chaos") if s == 200 else None


def converged(managers):
    idx = [((m.cluster() or {}).get("applied_index")) for m in managers]
    return idx if None not in idx and max(idx) - min(idx) <= 2 else None


def all_healthy(m, limit=None):
    s, v = m.api("GET", "/v1/nodes", timeout=8)
    if s != 200:
        return None
    bad = [n["id"][:8] for n in v if (n.get("liveness") or {}).get("health") != "healthy"]
    return True if not bad else None


def by_addr(managers, addr):
    return next((m for m in managers if addr and addr.split(":")[0] == m.ip()), None)


# Scenarios. Each takes (cfg, managers, workers, report) and leaves the cluster as it found it.

def leader_dies(c, ms, ws, r):
    print("\n== the leader's manager is stopped", flush=True)
    lead = by_addr(ms, ms[0].leader_addr())
    r.check("a leader to start with", lead is not None)
    if not lead:
        return
    others = [m for m in ms if m is not lead]
    r.note(f"stopping the manager on {lead.ip()}")
    lead.run("systemctl stop freightdock-manager")
    try:
        new, took = wait_for("a new leader", lambda: (lambda a: a if a and by_addr(ms, a) is not lead else None)(others[0].leader_addr()), 30, 0.5)
        r.check("the other two elect a leader", new is not None, f"{took:.1f}s")
        v = f"leader-dies-{int(time.time())}"
        s = probe_write(others[1], c["probe_node"], c["_base"], v)
        r.check("a write through a follower commits", s == 200, f"HTTP {s}")
        seen, _ = wait_for("seen", lambda: probe_read(others[0], c["probe_node"]) == v, 10)
        r.check("and is read on the other", bool(seen))
        healthy, took = wait_for("healthy", lambda: all_healthy(others[0]), 45)
        r.check("every node healthy again (its workers redial)", bool(healthy), f"{took:.1f}s")
    finally:
        lead.run("systemctl start freightdock-manager")
    back, took = wait_for("converged", lambda: converged(ms), 60)
    r.check("the stopped manager comes back and catches up", bool(back), f"{took:.1f}s")
    seen = probe_read(lead, c["probe_node"])
    r.check("it has the write made while it was down", seen == v, str(seen))


def follower_cut(c, ms, ws, r):
    print("\n== a follower is cut off from the other managers", flush=True)
    lead = by_addr(ms, ms[0].leader_addr())
    f = next(m for m in ms if m is not lead)
    rest = [m for m in ms if m is not f]
    r.note(f"cutting {f.ip()} from {', '.join(m.ip() for m in rest)}")
    r.check("the cut is made", cut(f, [m.ip() for m in rest], c["heal_s"]))
    try:
        time.sleep(3)
        v = f"follower-cut-{int(time.time())}"
        t0 = time.time()
        s = probe_write(f, c["probe_node"], c["_base"], v + "-lost")
        r.check("a write through the cut-off follower is refused, not taken", s != 200, f"HTTP {s} in {time.time() - t0:.1f}s")
        s = probe_write(lead, c["probe_node"], c["_base"], v)
        r.check("a write through the leader commits", s == 200, f"HTTP {s}")
        q, took = wait_for("not quorate", lambda: (lambda cl: cl is not None and not cl.get("quorate"))(f.cluster()), 20)
        r.check("the cut-off follower says it is not quorate", bool(q), f"{took:.1f}s")
    finally:
        heal(f)
    seen, took = wait_for("caught up", lambda: probe_read(f, c["probe_node"]) == v, 30)
    r.check("healed, it catches up with the write", bool(seen), f"{took:.1f}s")
    r.check("and the refused write is nowhere", probe_read(lead, c["probe_node"]) == v)


def holder_cut(c, ms, ws, r):
    print("\n== the manager holding the most workers' streams is cut off for two minutes", flush=True)
    def holds(m):
        s, nodes = m.api("GET", "/v1/nodes")
        return [n["id"] for n in nodes or [] if (n.get("liveness") or {}).get("peer") and not (n.get("liveness") or {}).get("via")] if s == 200 else []
    held = {m.ip(): holds(m) for m in ms}
    f = max(ms, key=lambda m: len(held[m.ip()]))
    if not held[f.ip()]:
        r.check("a manager holds a worker's stream", False)
        return
    lead = next((m for m in ms if m is not f), None)
    rest = [m for m in ms if m is not f]
    r.note(f"cutting {f.ip()} ({'the leader' if f.leader_addr() and by_addr(ms, f.leader_addr()) is f else 'a follower'}), which holds {len(held[f.ip()])} worker streams, from {', '.join(m.ip() for m in rest)}")
    r.check("the cut is made", cut(f, [m.ip() for m in rest], c["heal_s"]))
    t0 = time.time()
    try:
        def moved():
            lead = next((m for m in rest if m.leader_addr()), rest[0])
            s, nodes = lead.api("GET", "/v1/nodes")
            if s != 200:
                return None
            on_f = [n["id"][:8] for n in nodes if ((n.get("liveness") or {}).get("via") or "").split(":")[0] == f.ip()]
            sick = [n["id"][:8] for n in nodes if (n.get("liveness") or {}).get("health") != "healthy"]
            return True if not on_f and not sick else None
        ok, took = wait_for("moved", moved, 90, 2)
        r.check("its workers are sent away and heard by the majority, all healthy", bool(ok), f"{time.time() - t0:.1f}s after the cut")
        r.check("the cut-off manager holds none any more", not holds(f), ", ".join(x[:8] for x in holds(f)))
        fenced = [n["id"][:8] for n in (lead.api("GET", "/v1/nodes")[1] or []) if (n.get("fencing") or {}).get("state") not in (None, "none")]
        r.check("no node was fenced for it", not fenced, ", ".join(fenced))
        while time.time() - t0 < 120:
            time.sleep(2)
    finally:
        heal(f)
    back, took = wait_for("converged", lambda: converged(ms), 60)
    r.check("healed, it catches up", bool(back), f"{took:.1f}s")


def leader_cut(c, ms, ws, r):
    print("\n== the leader is cut off from the other managers", flush=True)
    lead = by_addr(ms, ms[0].leader_addr())
    rest = [m for m in ms if m is not lead]
    r.note(f"cutting {lead.ip()} from {', '.join(m.ip() for m in rest)}")
    r.check("the cut is made", cut(lead, [m.ip() for m in rest], c["heal_s"]))
    v = f"leader-cut-{int(time.time())}"
    try:
        new, took = wait_for("new leader", lambda: (lambda a: a if a and by_addr(ms, a) is not lead else None)(rest[0].leader_addr()), 30, 0.5)
        r.check("the majority elects another leader", new is not None, f"{took:.1f}s")
        t0 = time.time()
        s = probe_write(lead, c["probe_node"], c["_base"], v + "-lost")
        r.check("the old leader refuses a write, not takes it", s != 200, f"HTTP {s} in {time.time() - t0:.1f}s")
        s = probe_write(rest[0], c["probe_node"], c["_base"], v)
        r.check("the new majority takes one", s == 200, f"HTTP {s}")
        q, took = wait_for("stepped down", lambda: (lambda cl: cl is not None and not cl.get("quorate"))(lead.cluster()), 20)
        r.check("the old leader says it is not quorate", bool(q), f"{took:.1f}s")
        healthy, took = wait_for("healthy", lambda: all_healthy(rest[0]), 60)
        r.check("every node healthy, heard by the majority", bool(healthy), f"{took:.1f}s")
    finally:
        heal(lead)
    seen, took = wait_for("caught up", lambda: probe_read(lead, c["probe_node"]) == v, 30)
    r.check("healed, the old leader has the majority's write", bool(seen), f"{took:.1f}s")
    back, took = wait_for("converged", lambda: converged(ms), 30)
    r.check("the three agree", bool(back))


def worker_cut(c, ms, ws, r):
    print("\n== a worker is cut off from every manager", flush=True)
    wc = c["worker_cut"]
    w = next(x for x in ws if x.name == wc["host"])
    node = wc["node"]
    m = ms[0]
    mine = lambda: [n for n, hosts in c["_watch"].seen.items() if w.name in hosts]
    # A singleton of the harness's own, free to leave the node (the cluster's own stacks may all be tied to it).
    compose = ("services:\n  one:\n    image: busybox:latest\n    command: [\"sh\", \"-c\", \"while true; do sleep 5; done\"]\n"
               f"    deploy:\n      placement:\n        preferences:\n          - prefer: node.id=={node}\n")
    st, _ = m.api("POST", "/v1/stacks", {"name": "chaos", "compose": compose}, timeout=60)
    r.check("a stack of its own, preferring the node, is deployed", st in (200, 201), f"HTTP {st}")
    c["_watch"].singletons.add("chaos-one-1")
    on, took = wait_for("on it", lambda: w.name in c["_watch"].seen.get("chaos-one-1", []), 120, 2)
    r.check("and runs there", bool(on), f"{took:.1f}s")
    held = mine()
    r.note(f"{w.ip()} runs {', '.join(held) or 'no singleton'}")
    r.check("the cut is made", cut(w, [x.ip() for x in ms], c["heal_s"]))
    t0 = time.time()
    try:
        un, took = wait_for("unreachable", lambda: (lambda s_v: s_v[0] == 200 and (s_v[1].get("liveness") or {}).get("health") == "unreachable")(m.api("GET", f"/v1/nodes/{node}")), 60)
        r.check("the managers call it unreachable", bool(un), f"{took:.1f}s")
        gone, took = wait_for("stopped", lambda: not mine() and not any(n in held for n in w_names(w)), 120, 2)
        r.check("it stops its own singletons (self-fencing)", bool(gone), f"{time.time() - t0:.1f}s after the cut")
        # Only what the managers moved runs again elsewhere: one a stack ties to this node stays, and says why.
        def placed():
            st, stacks = m.api("GET", "/v1/stacks")
            return {x["container_name"]: x.get("node_id") for k in (stacks or []) for x in k.get("workloads", [])} if st == 200 else {}
        fenced, took = wait_for("fenced", lambda: (lambda s_v: s_v[0] == 200 and (s_v[1].get("fencing") or {}).get("state") == "fenced")(m.api("GET", f"/v1/nodes/{node}")), 120, 2)
        r.check("the managers fence it", bool(fenced), f"{time.time() - t0:.1f}s after the cut")
        time.sleep(5)
        p = placed()
        movers = [n for n in held if p.get(n) and p[n] != node]
        stay = [n for n in held if n not in movers]
        r.note(f"moved by the managers: {', '.join(movers) or 'none'}; kept for this node: {', '.join(stay) or 'none'}")
        moved, took = wait_for("moved", lambda: all(any(h != w.name for h in c["_watch"].seen.get(n, [])) for n in movers) or None, 180, 2)
        r.check("what was moved runs again elsewhere", bool(moved), f"{time.time() - t0:.1f}s after the cut")
    finally:
        heal(w)
    # A provider fence (Proxmox, Redfish) powered it off and leaves it off, as it should: it is powered on again
    # here when the config says how.
    on = wc.get("power_on")
    if on and w.run("true", timeout=8)[0] != 0:
        code, _ = Host(c.get("ssh", ["ssh"]), {"host": on["host"]}).run(on["cmd"], timeout=60)
        r.note(f"it was powered off; powered on again: exit {code}")
    def fenced_state():
        s, v = m.api("GET", f"/v1/nodes/{node}")
        return (v.get("fencing") or {}).get("state") if s == 200 else None
    back, took = wait_for("healthy", lambda: (lambda s_v: s_v[0] == 200 and (s_v[1].get("liveness") or {}).get("health") == "healthy")(m.api("GET", f"/v1/nodes/{node}")), 240, 2)
    r.check("healed, it is heard again", bool(back), f"{took:.1f}s (it may have rebooted)")
    st, took = wait_for("authorized", lambda: fenced_state() == "none", 120, 2)
    r.check("and is let back in (authorized)", bool(st), f"fencing {fenced_state()}")
    gone, took = wait_for("old copy removed", lambda: "chaos-one-1" not in w_names(w) or None, 60, 2)
    r.check("the moved workload's old container is not started again there", bool(gone))
    s_, _ = m.api("DELETE", "/v1/stacks/chaos", timeout=60)
    r.note(f"the harness's stack removed: HTTP {s_}")


def w_names(w):
    code, o = w.run("docker ps --format '{{.Names}}'", timeout=8)
    return o.split() if code == 0 else []


SCENARIOS = {"leader-dies": leader_dies, "follower-cut": follower_cut, "holder-cut": holder_cut, "leader-cut": leader_cut, "worker-cut": worker_cut}


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    cfg = json.load(open(sys.argv[1]))
    names = sys.argv[2:] or list(SCENARIOS)
    unknown = [n for n in names if n not in SCENARIOS]
    if unknown:
        print(f"no scenario {', '.join(unknown)}; there are {', '.join(SCENARIOS)}")
        return 2
    ssh = cfg.get("ssh", ["ssh"])
    cfg.setdefault("heal_s", 600)
    ms = [Manager(ssh, x) for x in cfg["managers"]]
    ws = [Host(ssh, x) for x in cfg["workers"]]
    r = Report()

    s, n = ms[0].api("GET", f"/v1/nodes/{cfg['probe_node']}")
    if s != 200:
        print(f"the probe node is not known to {ms[0].ip()} (HTTP {s})")
        return 2
    cfg["_base"] = {k: v for k, v in (n.get("labels") or {}).items() if k != "chaos"}
    if not converged(ms) or not all_healthy(ms[0]):
        print("the cluster is not healthy and agreed to start with; nothing done")
        return 2

    watch = Watcher(ws, cfg.get("singletons", []))
    cfg["_watch"] = watch
    watch.start()
    time.sleep(3)
    try:
        for name in names:
            SCENARIOS[name](cfg, ms, ws, r)
            wait_for("settled", lambda: converged(ms) and all_healthy(ms[0]), 120, 2)
    finally:
        for h in ms + ws:
            heal(h)
        watch.finish()
        lead = by_addr(ms, ms[0].leader_addr()) or ms[0]
        s, _ = lead.api("PUT", f"/v1/nodes/{cfg['probe_node']}/labels", {"labels": cfg["_base"]})
        print(f"\nthe probe node's labels given back: HTTP {s}")
    r.check("no singleton ever ran on two nodes at once", not watch.split,
            "; ".join(f"{t} {n} on {', '.join(h)}" for t, n, h in watch.split[:5]))
    passed = sum(1 for ok, _, _ in r.rows if ok)
    print(f"\n{passed} of {len(r.rows)} checks passed")
    return 1 if r.failed else 0


if __name__ == "__main__":
    sys.exit(main())
