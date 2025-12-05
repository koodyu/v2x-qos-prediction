#!/usr/bin/env python3
# collect_data.py - High-Fidelity Data Collection for V2X AI Training
#
# Key Features for AI:
#   1. Causal Capturing: Records both Cause (Switch Queue) and Effect (RTT).
#   2. Reliability Monitoring: Captures Switch Drops (SLA violations).
#   3. Topology Awareness: Auto-detects uplink bottlenecks (no hardcoded eth1).
#   4. Time-Series Stability: Uses monotonic clocks and non-blocking I/O.

import time
import csv
import os
import random
import signal
import sys
import threading

from mininet.net import Mininet
from mininet.node import OVSKernelSwitch, Controller
from mininet.link import TCLink
from mininet.log import setLogLevel

from v2x_env import HybridV2XTopo, MEC_IP
from utils import (
    get_interface_stats,
    get_queue_stats,
    ping_once,
    start_iperf_servers,
)
from traffic_generator import (
    run_traffic_scenario,
    get_traffic_status,
    BASE_PORT,
)

# ============================================================
# Configuration
# ============================================================
OUTPUT_FILE = "telemetry_dataset.csv"
DURATION = 2400.0          # Total experiment time (seconds)
SAMPLE_INTERVAL = 0.5     # Target loop interval
N_HOSTS = 14
CORE_SWITCH_NAME = "s_core"  # Name of the core switch to identify uplinks

shutdown = False


# ============================================================
# Helpers
# ============================================================
def signal_handler(sig, frame):
    global shutdown
    print("\n>>> Caught signal, shutting down...")
    shutdown = True


def get_zone(node_id):
    """Maps Host ID to Zone Name."""
    if node_id <= 4:
        return "highway"
    elif node_id <= 10:
        return "urban"
    return "suburb"


def normalize_scenario_name(raw_name):
    """Normalizes traffic scenario labels for classification tasks."""
    if not raw_name:
        return "idle"
    if raw_name.startswith("A-Highway"):
        return "A-Highway"
    if raw_name.startswith("B-Urban"):
        return "B-Urban"
    if raw_name.startswith("C-Suburb"):
        return "C-Suburb"
    return "idle"


def find_uplink_interface(switch_node, core_name):
    """
    Dynamically finds the interface on 'switch_node' that connects to 'core_name'.
    Crucial for correctly identifying the bottleneck queue without guessing 'eth1'.
    """
    for intf in switch_node.intfList():
        link = intf.link
        if link:
            n1 = link.intf1.node
            n2 = link.intf2.node
            if n1.name == core_name or n2.name == core_name:
                return intf.name
    return None


# ============================================================
# Main
# ============================================================
def main():
    global shutdown

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    setLogLevel("info")
    random.seed(42)  # Seed for reproducibility

    # 1. Build network
    print(">>> Building Mininet network...")
    topo = HybridV2XTopo()
    net = Mininet(
        topo=topo,
        link=TCLink,
        switch=OVSKernelSwitch,
        controller=Controller,
    )
    net.start()
    time.sleep(2)

    mec = net.get("mec")
    hosts = {i: net.get("h{}".format(i)) for i in range(1, N_HOSTS + 1)}

    # --------------------------------------------------------
    # [CRITICAL] Identify Bottlenecks (Zone Switch -> Core)
    # --------------------------------------------------------
    print(">>> Detecting topology bottlenecks...")
    sw_hw = net.get("s_hw")
    sw_urb = net.get("s_urb")
    sw_sub = net.get("s_sub")

    # Auto-detect which interface connects to s_core
    hw_intf = find_uplink_interface(sw_hw, CORE_SWITCH_NAME)
    urb_intf = find_uplink_interface(sw_urb, CORE_SWITCH_NAME)
    sub_intf = find_uplink_interface(sw_sub, CORE_SWITCH_NAME)

    if not (hw_intf and urb_intf and sub_intf):
        print("!!! ERROR: Could not find uplinks to s_core. Check topology!")
        net.stop()
        return

    print("   -> Highway uplink: {}-{}".format(sw_hw.name, hw_intf))
    print("   -> Urban   uplink: {}-{}".format(sw_urb.name, urb_intf))
    print("   -> Suburb  uplink: {}-{}".format(sw_sub.name, sub_intf))

    # Map zones to (SwitchObject, InterfaceName)
    switch_uplinks = {
        "highway": (sw_hw, hw_intf),
        "urban":   (sw_urb, urb_intf),
        "suburb":  (sw_sub, sub_intf),
    }

    # 2. Start iperf3 servers
    print(">>> Starting iperf3 servers on MEC (ports {}-{})...".format(
        BASE_PORT + 1, BASE_PORT + N_HOSTS
    ))
    start_iperf_servers(mec, N_HOSTS, base_port=BASE_PORT)
    time.sleep(1)

    # 3. Start traffic generator
    print(">>> Starting traffic generator thread...")
    tg_thread = threading.Thread(
        target=run_traffic_scenario,
        args=(net, DURATION + 5.0),
    )
    tg_thread.daemon = True
    tg_thread.start()

    # 4. Initialize Stats Tracking
    # Structure: node_id -> (tx_bytes, rx_bytes, tx_dropped)
    prev_host_stats = {}
    # Structure: node_id -> total_drops (for host queue)
    prev_host_qdrops = {}
    # Structure: zone_name -> total_drops (for switch queue)
    prev_switch_qdrops = {}

    # Init Host Stats
    for i in range(1, N_HOSTS + 1):
        host = hosts[i]
        intf = "h{}-eth0".format(i)
        prev_host_stats[i] = get_interface_stats(host, intf)
        _, qd = get_queue_stats(host, intf)
        prev_host_qdrops[i] = qd

    # Init Switch Stats
    for z_name, (sw_obj, sw_intf) in switch_uplinks.items():
        _, sw_qd = get_queue_stats(sw_obj, sw_intf)
        prev_switch_qdrops[z_name] = sw_qd

    # 5. Main Loop
    print(">>> Starting experiment for {} seconds...".format(DURATION))

    rows_written = 0
    start_time_wall = time.time()       # For absolute timestamp (wall clock)
    start_time_mono = time.monotonic()  # For accurate interval calc (monotonic)
    last_loop_time = start_time_mono

    # Use newline='' for proper CSV formatting on all OS
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp", "dt", "node_id", "zone",
            "rtt_ms", "tx_mbps", "rx_mbps",
            "host_queue_depth", "host_queue_drops",
            "switch_queue_depth", "switch_queue_drops",
            "scenario", "is_active",
        ])

        while not shutdown:
            loop_start = time.monotonic()
            elapsed_wall = time.time() - start_time_wall

            if elapsed_wall >= DURATION:
                break

            # Calculate accurate dt (delta time)
            dt = loop_start - last_loop_time
            if dt <= 0.0:
                dt = SAMPLE_INTERVAL  # First loop or clock anomaly

            # Get Global Traffic State
            status = get_traffic_status()
            norm_scenario = normalize_scenario_name(
                status.get("current_scenario")
            )
            active_nodes = status.get("active_nodes", [])

            # --------------------------------------------------
            # Phase 1: Poll Switch Queues (The Causal Source)
            # --------------------------------------------------
            curr_sw_queues = {}      # zone -> depth
            curr_sw_drops_delta = {} # zone -> drop_delta

            for z_name, (sw_obj, sw_intf) in switch_uplinks.items():
                sw_depth, sw_total_drops = get_queue_stats(sw_obj, sw_intf)

                # Calculate drop delta for this interval
                prev_drop = prev_switch_qdrops.get(z_name, 0)
                drop_delta = sw_total_drops - prev_drop
                if drop_delta < 0:
                    drop_delta = 0

                curr_sw_queues[z_name] = sw_depth
                curr_sw_drops_delta[z_name] = drop_delta

                # Update history
                prev_switch_qdrops[z_name] = sw_total_drops

            # --------------------------------------------------
            # Phase 2: Poll Hosts (The Effects)
            # --------------------------------------------------
            for node_id, host in hosts.items():
                intf = "h{}-eth0".format(node_id)
                zone = get_zone(node_id)
                host_name = "h{}".format(node_id)

                # Labeling
                if host_name in active_nodes and norm_scenario != "idle":
                    scenario = norm_scenario
                    is_active = 1
                else:
                    scenario = "idle"
                    is_active = 0

                # 1. Ping (Non-blocking, timeout enforced by v2x_utils.ping_once)
                rtt_ms = ping_once(host, MEC_IP, timeout_sec=0.1)

                # 2. Host Stats
                curr_tx, curr_rx, _ = get_interface_stats(host, intf)
                h_q_depth, h_total_drops = get_queue_stats(host, intf)

                # 3. Calculate Rates & Deltas
                prev_tx, prev_rx, _ = prev_host_stats[node_id]
                prev_h_drop = prev_host_qdrops[node_id]

                tx_diff = curr_tx - prev_tx
                rx_diff = curr_rx - prev_rx
                if tx_diff < 0:
                    tx_diff = 0
                if rx_diff < 0:
                    rx_diff = 0

                # Throughput (Mbps) - using accurate dt
                tx_mbps = tx_diff * 8.0 / (dt * 1e6)
                rx_mbps = rx_diff * 8.0 / (dt * 1e6)

                if tx_mbps < 0.0:
                    tx_mbps = 0.0
                if rx_mbps < 0.0:
                    rx_mbps = 0.0

                # Host Drops (Delta)
                h_drop_delta = h_total_drops - prev_h_drop
                if h_drop_delta < 0:
                    h_drop_delta = 0

                # 4. Get corresponding Switch metrics (Broadcast zone stats to host)
                s_q_depth = curr_sw_queues.get(zone, 0)
                s_drop_delta = curr_sw_drops_delta.get(zone, 0)

                # Update history
                prev_host_stats[node_id] = (curr_tx, curr_rx, 0)
                prev_host_qdrops[node_id] = h_total_drops

                # Write CSV Row
                writer.writerow([
                    round(elapsed_wall, 3),
                    round(dt, 4),
                    host_name,
                    zone,
                    round(rtt_ms, 1),
                    round(tx_mbps, 2),
                    round(rx_mbps, 2),
                    h_q_depth,
                    h_drop_delta,
                    s_q_depth,
                    s_drop_delta,
                    scenario,
                    is_active,
                ])
                rows_written += 1

            # Update loop timer
            last_loop_time = loop_start

            # Sleep control to keep ~SAMPLE_INTERVAL sampling
            elapsed_in_loop = time.monotonic() - loop_start
            sleep_time = SAMPLE_INTERVAL - elapsed_in_loop
            if sleep_time < 0.01:
                sleep_time = 0.01
            time.sleep(sleep_time)

    # 6. Cleanup
    print(">>> Experiment complete. Rows written: {}".format(rows_written))
    net.stop()
    os.system("pkill -9 iperf3 2>/dev/null")
    os.system("mn -c > /dev/null 2>&1")
    print(">>> Cleanup done.")


if __name__ == "__main__":
    main()
