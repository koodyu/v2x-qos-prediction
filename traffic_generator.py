#!/usr/bin/env python3
# traffic_generator.py - Stochastic Traffic Simulation for V2X
#
# Design goals:
#   - Keep three "scenario types":
#       A: Highway chain (h1 -> h4 in order)
#       B: Urban burst at center + diffusion to neighbors
#       C: Suburb low-load with occasional medium bursts
#   - Randomize bandwidth, duration, and number of participating hosts
#     so that each run produces different traffic while preserving the pattern.
#   - One host -> one iperf3 server port: BASE_PORT + host_index
#   - Node-level locks to avoid concurrent node.cmd() calls.

import time
import random
import threading
import traceback
import re

from v2x_env import MEC_IP  # Keep MEC IP consistent with topology

BASE_PORT = 5000  # server ports: 5001..50014 for h1..h14

# =============== Global traffic status ===============
traffic_status = {
    "running": False,
    "current_scenario": None,
    "active_nodes": [],
    "cycle_count": 0,
}
status_lock = threading.Lock()

# Node-level locks to protect node.cmd()
node_locks = {}
node_locks_init = threading.Lock()


def get_node_lock(node_name):
    """Get or create a lock for a specific Mininet node."""
    with node_locks_init:
        if node_name not in node_locks:
            node_locks[node_name] = threading.Lock()
        return node_locks[node_name]


def get_traffic_status():
    """Thread-safe getter for traffic_status."""
    with status_lock:
        return traffic_status.copy()


def update_status(scenario, nodes):
    """Thread-safe update of current scenario and active nodes."""
    with status_lock:
        traffic_status["current_scenario"] = scenario
        traffic_status["active_nodes"] = list(nodes) if nodes else []


def _host_index_from_name(name):
    """
    Extract integer index from a host name like 'h6' -> 6.
    Returns None if parsing fails.
    """
    try:
        m = re.match(r"h(\d+)$", name)
        if not m:
            return None
        return int(m.group(1))
    except Exception:
        return None


def start_iperf_flow(host, bandwidth_mbps, duration_sec, log_suffix=""):
    """
    Start an iperf3 UDP flow from 'host' to MEC with node-level locking.

    Requirements:
      - MEC node must be running iperf3 servers:
            BASE_PORT + i   for host hi
        Example: h1 -> 5001, h2 -> 5002, ..., h14 -> 5014.
    """
    if host is None:
        return False

    node_name = host.name
    idx = _host_index_from_name(node_name)
    if idx is None:
        print("   [WARN] Unknown host name format: {}".format(node_name))
        return False

    port = BASE_PORT + idx
    lock = get_node_lock(node_name)

    lock.acquire()
    try:
        log_file = "/tmp/iperf_{}{}_{}.log".format(node_name, log_suffix, port)
        cmd = (
            "iperf3 -c {ip} -p {port} -u -b {bw}M -t {dur} "
            "> {log} 2>&1 &"
        ).format(
            ip=MEC_IP,
            port=port,
            bw=bandwidth_mbps,
            dur=duration_sec,
            log=log_file,
        )
        host.cmd(cmd)
        return True
    except Exception as e:
        print("   [ERROR] iperf on {}: {}".format(node_name, e))
        return False
    finally:
        lock.release()


def safe_get_host(net, name):
    """Safely get a host from the Mininet network."""
    try:
        return net.get(name)
    except Exception:
        return None


# =============== Scenario parameters (random ranges) ===============

# Highway: chain pattern, fixed order h1 -> h4
HW_NODES = ["h1", "h2", "h3", "h4"]
HW_BW_MIN = 60.0   # Mbps
HW_BW_MAX = 100.0
HW_DUR_MIN = 4.0   # seconds
HW_DUR_MAX = 10.0
HW_GAP_MIN = 2.0   # inter-start gap (can cause overlap)
HW_GAP_MAX = 6.0

# Urban: center burst + diffusion to neighbors
URBAN_CENTER = "h6"
URBAN_NEIGHBORS = ["h5", "h7", "h9", "h10"]
URB_CENTER_BW_MIN = 30.0
URB_CENTER_BW_MAX = 80.0
URB_CENTER_DUR_MIN = 6.0
URB_CENTER_DUR_MAX = 15.0

URB_NEI_BW_MIN = 10.0
URB_NEI_BW_MAX = 50.0
URB_NEI_DUR_MIN = 4.0
URB_NEI_DUR_MAX = 12.0
URB_DIFFUSE_DELAY_MIN = 1.0  # delay before neighbors start
URB_DIFFUSE_DELAY_MAX = 3.0

# Suburb: low-load majority + occasional medium load
SUBURB_NODES = ["h11", "h12", "h13", "h14"]
SUB_BW_MIN = 10.0
SUB_BW_MAX = 60.0
SUB_DUR_MIN = 3.0
SUB_DUR_MAX = 10.0


def run_traffic_scenario(net, duration_limit=None):
    """
    Main traffic generation loop.

    Design:
      - Keep three logical patterns:
          A: Highway chain
          B: Urban center-burst + neighbor diffusion
          C: Suburb low-load biased random traffic
      - Randomize parameters within given ranges in each cycle.
      - Communicate with collect_data.py via get_traffic_status()/update_status().

    Args:
        net: Mininet network object (already started).
        duration_limit: Optional global time limit (seconds). If exceeded, exit loop.
    """
    global traffic_status

    print(">>> [Traffic Gen] Starting. Waiting 3s for network stability...")
    time.sleep(3.0)

    if net is None:
        print(">>> [Traffic Gen] ERROR: Network object is None!")
        return

    # Cache all hosts
    print(">>> [Traffic Gen] Validating hosts...")
    valid_hosts = {}
    for i in range(1, 15):
        name = "h{}".format(i)
        h = safe_get_host(net, name)
        if h is not None:
            valid_hosts[name] = h
            get_node_lock(name)  # pre-create lock
            print("   -> {} OK".format(name))

    with status_lock:
        traffic_status["running"] = True

    start_time = time.time()
    cycle = 0

    try:
        while True:
            cycle += 1
            with status_lock:
                traffic_status["cycle_count"] = cycle

            elapsed = time.time() - start_time
            if duration_limit is not None and elapsed > duration_limit:
                print(">>> [Traffic Gen] Duration limit reached.")
                break

            # ==========================================
            # Scenario A: Highway chain (fixed order, random params)
            # ==========================================
            print("\n>>> [Cycle {}] Scenario A: Highway Chain (randomized)".format(cycle))
            active = []

            for node_name in HW_NODES:
                elapsed = time.time() - start_time
                if duration_limit is not None and elapsed > duration_limit:
                    break

                host = valid_hosts.get(node_name)
                if host is None:
                    continue

                bw = random.uniform(HW_BW_MIN, HW_BW_MAX)
                dur = random.uniform(HW_DUR_MIN, HW_DUR_MAX)
                gap = random.uniform(HW_GAP_MIN, HW_GAP_MAX)

                active.append(node_name)
                update_status("A-Highway", active)

                print(
                    "   -> {} sending {:.1f} Mbps for {:.1f}s (gap ≈ {:.1f}s)".format(
                        node_name, bw, dur, gap
                    )
                )
                start_iperf_flow(host, bw, dur, "_hw")

                # Inter-flow gap (allows partial overlap)
                time.sleep(gap)

            print("   -> Highway scenario complete.")
            update_status("A-Highway-done", [])
            time.sleep(2.0)

            # ==========================================
            # Scenario B: Urban burst + diffusion
            # ==========================================
            elapsed = time.time() - start_time
            if duration_limit is not None and elapsed > duration_limit:
                break

            print(
                "\n>>> [Cycle {}] Scenario B: Urban Burst + Diffusion (randomized)".format(
                    cycle
                )
            )

            active = []
            max_dur = 0.0

            # 1) Center burst at h6
            center_host = valid_hosts.get(URBAN_CENTER)
            if center_host is not None:
                c_bw = random.uniform(URB_CENTER_BW_MIN, URB_CENTER_BW_MAX)
                c_dur = random.uniform(URB_CENTER_DUR_MIN, URB_CENTER_DUR_MAX)
                max_dur = max(max_dur, c_dur)

                active.append(URBAN_CENTER)
                update_status("B-Urban", active)

                print(
                    "   -> {} epicenter: {:.1f} Mbps for {:.1f}s".format(
                        URBAN_CENTER, c_bw, c_dur
                    )
                )
                start_iperf_flow(center_host, c_bw, c_dur, "_urb_c")

            # Small delay before diffusion starts
            delay = random.uniform(URB_DIFFUSE_DELAY_MIN, URB_DIFFUSE_DELAY_MAX)
            time.sleep(delay)

            # 2) Random subset of neighbors start sending
            neighbor_candidates = [n for n in URBAN_NEIGHBORS if n in valid_hosts]
            if len(neighbor_candidates) > 0:
                k = random.randint(1, len(neighbor_candidates))
                targets = random.sample(neighbor_candidates, k)
                print("   -> Diffusion to neighbors: {}".format(targets))

                for name in targets:
                    host = valid_hosts.get(name)
                    if host is None:
                        continue

                    b = random.uniform(URB_NEI_BW_MIN, URB_NEI_BW_MAX)
                    d = random.uniform(URB_NEI_DUR_MIN, URB_NEI_DUR_MAX)
                    max_dur = max(max_dur, d)

                    active.append(name)
                    update_status("B-Urban", active)

                    print(
                        "      - {} sending {:.1f} Mbps for {:.1f}s".format(
                            name, b, d
                        )
                    )
                    start_iperf_flow(host, b, d, "_urb_n")

            # Wait until most Urban flows are done
            if max_dur > 0:
                time.sleep(max_dur + 1.0)

            print("   -> Urban scenario complete.")
            update_status("B-Urban-done", [])
            time.sleep(2.0)

            # ==========================================
            # Scenario C: Suburb low-load random
            # ==========================================
            elapsed = time.time() - start_time
            if duration_limit is not None and elapsed > duration_limit:
                break

            print(
                "\n>>> [Cycle {}] Scenario C: Suburb Random (low-load biased)".format(
                    cycle
                )
            )

            suburb_hosts = [n for n in SUBURB_NODES if n in valid_hosts]
            if len(suburb_hosts) > 0:
                # Randomly pick 1–3 suburban hosts
                k = random.randint(1, min(3, len(suburb_hosts)))
                targets = random.sample(suburb_hosts, k)
                active = list(targets)
                update_status("C-Suburb", active)

                print("   -> Suburb targets: {}".format(targets))

                max_dur_sub = 0.0
                for name in targets:
                    host = valid_hosts.get(name)
                    if host is None:
                        continue

                    bw = random.uniform(SUB_BW_MIN, SUB_BW_MAX)
                    dur = random.uniform(SUB_DUR_MIN, SUB_DUR_MAX)
                    max_dur_sub = max(max_dur_sub, dur)

                    print(
                        "      - {} sending {:.1f} Mbps for {:.1f}s".format(
                            name, bw, dur
                        )
                    )
                    start_iperf_flow(host, bw, dur, "_sub")

                if max_dur_sub > 0:
                    time.sleep(max_dur_sub + 1.0)

            print("   -> Suburb scenario complete.")
            update_status("C-Suburb-done", [])
            time.sleep(3.0)

    except Exception as e:
        print(">>> [Traffic Gen] Error: {}".format(e))
        traceback.print_exc()
    finally:
        with status_lock:
            traffic_status["running"] = False
            traffic_status["current_scenario"] = "stopped"
            traffic_status["active_nodes"] = []
        print(">>> [Traffic Gen] Finished {} cycles.".format(cycle))
