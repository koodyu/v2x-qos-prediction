#!/usr/bin/env python3
# v2x_env.py - Physical Topology Definition (with explicit dpids)

from mininet.topo import Topo
from mininet.link import TCLink

# Core configuration (MEC <-> core switch)
BW_CORE = 200        # Mbps
DELAY_CORE = "1ms"
QUEUE_CORE = 1000

# Zone uplink configuration (zone switch <-> core switch)
BW_ZONE_UPLINK = 100  # Mbps
DELAY_ZONE = "2ms"
QUEUE_ZONE = 500

# Access link configuration (host <-> zone switch)
BW_ACCESS_HW = 80     # highway RSUs
BW_ACCESS_URB = 40    # urban RSUs
BW_ACCESS_SUB = 30    # suburban RSUs
ACCESS_DELAY = "2ms"
QUEUE_ACCESS = 200

MEC_IP = "10.0.0.100"


class HybridV2XTopo(Topo):
    """
    Hybrid V2X topology:
      - 1 MEC host connected to a core switch (s_core)
      - 3 zone switches: s_hw (highway), s_urb (urban), s_sub (suburban)
      - 14 RSU hosts: h1-h4 (highway), h5-h10 (urban), h11-h14 (suburban)
    """

    def build(self):
        # Switches with explicit dpids because names are non-canonical
        s_core = self.addSwitch("s_core", dpid="0000000000000001")
        s_hw   = self.addSwitch("s_hw",   dpid="0000000000000002")
        s_urb  = self.addSwitch("s_urb",  dpid="0000000000000003")
        s_sub  = self.addSwitch("s_sub",  dpid="0000000000000004")

        # MEC host
        mec = self.addHost("mec", ip=MEC_IP)

        # Core link
        self.addLink(
            mec,
            s_core,
            cls=TCLink,
            bw=BW_CORE,
            delay=DELAY_CORE,
            max_queue_size=QUEUE_CORE,
            use_htb=True,
        )

        # Zone uplinks
        for sw in (s_hw, s_urb, s_sub):
            self.addLink(
                sw,
                s_core,
                cls=TCLink,
                bw=BW_ZONE_UPLINK,
                delay=DELAY_ZONE,
                max_queue_size=QUEUE_ZONE,
                use_htb=True,
            )

        # Highway RSUs: h1-h4
        for i in range(1, 5):
            self._add_rsu(i, s_hw, BW_ACCESS_HW)

        # Urban RSUs: h5-h10
        for i in range(5, 11):
            self._add_rsu(i, s_urb, BW_ACCESS_URB)

        # Suburban RSUs: h11-h14
        for i in range(11, 15):
            self._add_rsu(i, s_sub, BW_ACCESS_SUB)

    def _add_rsu(self, index, switch, bw_mbps):
        """
        Add one RSU host and connect it to a zone switch.

        Host naming rule:
          h1 -> 10.0.0.1
          h2 -> 10.0.0.2
          ...
        """
        name = "h{}".format(index)
        ip = "10.0.0.{}".format(index)

        host = self.addHost(name, ip=ip)
        self.addLink(
            host,
            switch,
            cls=TCLink,
            bw=bw_mbps,
            delay=ACCESS_DELAY,
            max_queue_size=QUEUE_ACCESS,
            use_htb=True,
        )


# Allow Mininet CLI to find this topology using '--topo v2x'
topos = {"v2x": (lambda: HybridV2XTopo())}
