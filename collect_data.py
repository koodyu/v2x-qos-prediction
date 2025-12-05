#!/usr/bin/env python3
# collect_data.py - Data collection for V2X topology (Optimized for AI/ML)
# Focus: High-fidelity telemetry, avoiding sampling bias and blocking I/O.

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
DURATION = 120.0          # total experiment time (seconds)
SAMPLE_INTERVAL = 0.2     # target loop interval (UPDATED from 0.5)
N_HOSTS = 14

shutdown = False


# ============================================================
# Helpers
# ============================================================

def signal_handler(sig, frame):
    global shutdown
    print("\n[!] Caught signal {}, shutting down gracefully...".format(sig))
    shutdown = True


def infer_zone(host_name):
    """
    Simple mapping: h1-4 -> highway, h5-10 -> urban, h11+ -> suburb
    （如果你在拓扑里已经设置 zone，可以改成用 host.params['zone']）
    """
    try:
        idx = int(host_name.lstrip("h"))
    except ValueError:
        return "unknown"

    if idx <= 4:
        return "highway"
    elif idx <= 10:
        return "urban"
    else:
        return "suburb"


def compute_mbps(prev_bytes, now_bytes, dt):
    """
    Convert byte counter delta to Mbps.
    """
    if prev_bytes is None or dt <= 0:
        return 0.0
    diff = now_bytes - prev_bytes
    if diff < 0:
        # counter wrap/reset
        return 0.0
    # bytes -> bits -> Mbps
    return 8.0 * diff / (dt * 1e6)


# ============================================================
# Core logic
# ============================================================

def collect_telemetry(net):
    """
    Main data collection loop.
    """
    global shutdown

    hosts = [net.get("h{}".format(i)) for i in range(1, N_HOSTS + 1)]
    mec = net.get("mec")

    # Start iperf servers on MEC
    start_iperf_servers(mec, N_HOSTS, base_port=BASE_PORT)

    # Start traffic generator in background
    tg_thread = threading.Thread(target=run_traffic_scenario, args=(net,))
    tg_thread.daemon = True
    tg_thread.start()

    # Prepare CSV
    need_header = not os.path.exists(OUTPUT_FILE)

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.writer(f)

        if need_header:
            # === HEADER（已加 rtt_loss）===
            writer.writerow([
                "timestamp",             # 相对实验起点的时间（秒）
                "dt",                    # 与上一采样点的时间差
                "host",
                "zone",
                "rtt_ms",
                "tx_mbps",
                "rx_mbps",
                "host_queue_depth",
                "host_queue_drops",
                "switch_queue_depth",
                "switch_queue_drops",
                "scenario",
                "is_critical",
                "rtt_loss",              # NEW: 0=ok, 1=timeout/failure
            ])

        # 保存上一次的 tx/rx 用来算 Mbps
        # key: (host_name, intf) -> (tx_bytes, rx_bytes, last_time)
        prev_bytes = {}

        start_time = time.time()
        last_sample_time = start_time

        print("[*] Start collecting telemetry for {:.1f}s...".format(DURATION))

        while not shutdown:
            now = time.time()
            t_sim = now - start_time
            if t_sim > DURATION:
                break

            dt = now - last_sample_time
            if dt <= 0:
                dt = SAMPLE_INTERVAL
            last_sample_time = now

            # 当前 traffic 状态（来自 traffic_generator）
            status = get_traffic_status() or {}
            scenario = status.get("scenario", "idle")
            is_critical = 1 if status.get("is_critical", False) else 0

            for h in hosts:
                host_name = h.name
                zone = infer_zone(host_name)
                intf = "{}-eth0".format(host_name)

                # ------------------------------
                # 1) Interface stats -> Mbps
                # ------------------------------
                tx_bytes, rx_bytes, _tx_dropped = get_interface_stats(h, intf)

                key = (host_name, intf)
                prev = prev_bytes.get(key)

                if prev:
                    prev_tx, prev_rx, prev_t = prev
                    dt_bytes = now - prev_t
                else:
                    prev_tx = prev_rx = None
                    dt_bytes = dt

                tx_mbps = compute_mbps(prev_tx, tx_bytes, dt_bytes)
                rx_mbps = compute_mbps(prev_rx, rx_bytes, dt_bytes)

                prev_bytes[key] = (tx_bytes, rx_bytes, now)

                # ------------------------------
                # 2) Queue stats
                # ------------------------------
                host_q_depth, host_q_drops = get_queue_stats(h, intf)

                # 如果你有明确的“核心交换机/瓶颈端口”，可以在这里改成从 switch 上取
                switch_q_depth = 0
                switch_q_drops = 0

                # ------------------------------
                # 3) RTT + loss
                # ------------------------------
                # 使用新的 utils.ping_once：返回 (rtt_ms, rtt_loss)
                rtt_ms, rtt_loss = ping_once(h, MEC_IP, timeout_sec=0.1)

                # 去掉 1000.0 魔法数，统一用 NaN 表示“测不到”
                if rtt_ms is None:
                    rtt_ms = float("nan")

                # ------------------------------
                # 4) 写 CSV（最后一列是 rtt_loss）
                # ------------------------------
                writer.writerow([
                    round(t_sim, 3),          # timestamp
                    round(dt, 4),             # dt
                    host_name,
                    zone,
                    rtt_ms,
                    round(tx_mbps, 3),
                    round(rx_mbps, 3),
                    host_q_depth,
                    host_q_drops,
                    switch_q_depth,
                    switch_q_drops,
                    scenario,
                    is_critical,
                    int(rtt_loss),
                ])

            # 控制采样周期（尽量贴近 SAMPLE_INTERVAL）
            loop_end = time.time()
            sleep_time = SAMPLE_INTERVAL - (loop_end - now)
            if sleep_time > 0:
                time.sleep(sleep_time)

        print("[*] Telemetry collection finished.")


def main():
    global shutdown

    setLogLevel("warning")
    signal.signal(signal.SIGINT, signal_handler)

    topo = HybridV2XTopo()
    net = Mininet(
        topo=topo,
        link=TCLink,
        controller=Controller,
        switch=OVSKernelSwitch,
        autoSetMacs=True,
        autoStaticArp=True,
    )

    try:
        net.start()
        collect_telemetry(net)
    finally:
        print("[*] Stopping Mininet...")
        net.stop()


if __name__ == "__main__":
    main()
