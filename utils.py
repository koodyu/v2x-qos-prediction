#!/usr/bin/env python3
# utils.py - Utility functions for V2X telemetry collection
# Focus: correctness and clarity over performance.

import re


def get_interface_stats(node, intf):
    """
    Read per-interface statistics inside the given Mininet node's namespace.

    Args:
        node: Mininet host or switch object.
        intf: Interface name string, e.g., "h1-eth0".

    Returns:
        (tx_bytes, rx_bytes, tx_dropped) as integers.
    """
    cmd = (
        "cat /sys/class/net/{intf}/statistics/tx_bytes "
        "/sys/class/net/{intf}/statistics/rx_bytes "
        "/sys/class/net/{intf}/statistics/tx_dropped 2>/dev/null"
    ).format(intf=intf)

    out = node.cmd(cmd).strip().split()
    if len(out) != 3:
        return 0, 0, 0

    try:
        tx = int(out[0])
        rx = int(out[1])
        dropped = int(out[2])
    except ValueError:
        return 0, 0, 0

    return tx, rx, dropped


def get_queue_stats(node, intf):
    """
    Read queue depth and total dropped packets from tc qdisc statistics.

    Args:
        node: Mininet host or switch object.
        intf: Interface name string, e.g., "h1-eth0".

    Returns:
        (queue_depth_packets, dropped_packets) as integers.
    """
    out = node.cmd("tc -s qdisc show dev {} 2>/dev/null".format(intf))

    q_match = re.search(r"backlog\s+\d+b\s+(\d+)p", out)
    d_match = re.search(r"dropped\s+(\d+)", out)

    q_depth = int(q_match.group(1)) if q_match else 0
    drops = int(d_match.group(1)) if d_match else 0

    return q_depth, drops


def parse_ping_rtt(ping_output):
    """
    Parse RTT from ping output.

    Args:
        ping_output: String output of the ping command.

    Returns:
        RTT in milliseconds as float, or None on failure/timeout.
    """
    try:
        # 典型格式: "time=12.345 ms"
        match = re.search(r"time=([\d\.]+)\s*ms", ping_output)
        if match:
            return float(match.group(1))
    except Exception:
        pass
    return None


def ping_once(node, target_ip, timeout_sec=0.2):
    """
    Send a single ICMP echo and return RTT and loss flag.

    Args:
        node: Mininet host object.
        target_ip: Destination IP string.
        timeout_sec: Maximum wall-clock time in seconds (float).
                     We use the 'timeout' command to enforce this.

    Returns:
        (rtt_ms, loss_flag)
          - rtt_ms   : float (RTT in ms) or None if timeout/failure
          - loss_flag: 0 if reply received, 1 if timeout/failure
    """
    # Sanitize timeout value
    try:
        t = float(timeout_sec)
    except (TypeError, ValueError):
        t = 0.2

    if t <= 0:
        t = 0.1

    # 用 'timeout' 控制整体执行时间
    # -c 1: 发送 1 个包
    # -W 1: ping 自身等待 1 秒；实际由 timeout t 提前杀掉
    cmd = "timeout {t:.3f} ping -c 1 -W 1 {ip} 2>/dev/null".format(
        t=t, ip=target_ip
    )

    out = node.cmd(cmd)
    rtt = parse_ping_rtt(out)

    if rtt is None:
        # 没有解析到 RTT，认为超时/丢包
        return None, 1
    else:
        return rtt, 0


def start_iperf_servers(mec_node, n_hosts, base_port=5000):
    """
    Start one iperf3 server per host on the MEC node.

    Args:
        mec_node: Mininet host object representing the MEC.
        n_hosts: Number of RSU hosts (e.g., 14).
        base_port: Base TCP/UDP port for iperf3 servers (default 5000).

    Effect:
        Starts iperf3 -s on ports base_port+1 ... base_port+n_hosts.
    """
    # Kill any previous iperf3 servers on the MEC
    mec_node.cmd("pkill -9 iperf3 2>/dev/null")

    for i in range(1, n_hosts + 1):
        port = base_port + i
        cmd = "iperf3 -s -p {} -D".format(port)
        mec_node.cmd(cmd)
