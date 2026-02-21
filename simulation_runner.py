"""
simulation_runner.py
--------------------
Main orchestrator for the DPC factory Multi-Agent System simulation.

Scenario (minimal)
------------------
  1 x ProducerAgent   – PV solar source (peak 80 kW)
  1 x ConsumerAgent   – Production machine (base load 50 kW)
  1 x StorageAgent    – BESS (200 kWh / 40 kW)

Agents each run their own asyncua.Server on a local loopback port.
The StorageAgent subscribes to the Producer's CurrentGeneration and
Consumer's CurrentPowerDemand via OPC UA data-change subscriptions,
feeding those values into its SoC decision logic.

A centralized DataLogger subscribes to the critical variable of every
agent and writes timestamped events to `simulation_results.csv`.

Usage
-----
    python simulation_runner.py

The simulation runs for SIMULATION_DURATION_SECONDS then shuts down cleanly.
"""

import asyncio
import csv
import logging
import os
import signal
from datetime import datetime

from asyncua import Client

from opcua_agent import OpcUaAgent
from factory_agents import ConsumerAgent, ProducerAgent, StorageAgent

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SIMULATION_DURATION_SECONDS = 60   # Total time to run the scenario
LOG_FILE = "simulation_results.csv"

ENDPOINTS = {
    "producer":  "opc.tcp://0.0.0.0:4840/dpc/producer",
    "consumer":  "opc.tcp://0.0.0.0:4841/dpc/consumer",
    "storage":   "opc.tcp://0.0.0.0:4842/dpc/storage",
}

# Remote access (localhost variant for clients connecting to servers above)
CLIENT_ENDPOINTS = {
    k: v.replace("0.0.0.0", "127.0.0.1") for k, v in ENDPOINTS.items()
}

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("simulation_runner")


# ---------------------------------------------------------------------------
# Centralized Data Logger
# ---------------------------------------------------------------------------

class DataLogger:
    """
    Connects to every agent endpoint as an OPC UA Client and subscribes to
    their key state variables. On datachange, timestamps and records the
    event to an in-memory buffer which is flushed to CSV on shutdown.
    """

    TRACKED_VARIABLES = {
        "producer":  ["CurrentGeneration", "OperationalState"],
        "consumer":  ["CurrentPowerDemand", "OperationalState"],
        "storage":   ["StateOfCharge", "OperationalState"],
    }

    def __init__(self, output_file: str = LOG_FILE):
        self._output_file = output_file
        self._rows: list[dict] = []
        self._clients: list[Client] = []
        self._subscriptions: list = []
        
        # Current state for dashboard visualization
        self._state = {
            "producer": {"CurrentGeneration": 0.0, "OperationalState": "Unknown"},
            "consumer": {"CurrentPowerDemand": 0.0, "OperationalState": "Unknown"},
            "storage": {"StateOfCharge": 0.0, "OperationalState": "Unknown"},
        }

    def _record(self, agent_key: str, variable: str, value):
        row = {
            "timestamp": datetime.now().isoformat(timespec="milliseconds"),
            "agent":     agent_key,
            "variable":  variable,
            "value":     value,
        }
        self._rows.append(row)
        logger.info(
            "[DataLogger] %s | %s.%s = %s",
            row["timestamp"], agent_key, variable, value,
        )
        
        # Update live state JSON for the web dashboard preview
        if agent_key in self._state and variable in self._state[agent_key]:
            self._state[agent_key][variable] = value
            try:
                import json
                with open("state.json", "w") as f:
                    json.dump(self._state, f)
            except Exception:
                pass

    def _make_callback(self, agent_key: str, var_name: str):
        def cb(node, val, data):
            self._record(agent_key, var_name, val)
        return cb

    async def _browse_variable(self, client: Client, var_name: str):
        """Walk peer address space and return the node matching var_name."""
        ns = await client.get_namespace_index(OpcUaAgent.NAMESPACE_URI)
        root = client.get_objects_node()
        children = await root.get_children()
        for obj in children:
            for child in await obj.get_children():
                bn = await child.read_browse_name()
                if bn.Name == var_name:
                    return child
        return None

    async def start(self, endpoints: dict):
        """Connect to all agents and start monitoring."""
        for agent_key, url in endpoints.items():
            try:
                c = Client(url)
                await c.__aenter__()
                self._clients.append(c)

                class LogHandler:
                    def __init__(self_, agent_key_, tracked_vars_, record_fn):
                        self_._agent_key   = agent_key_
                        self_._tracked     = tracked_vars_
                        self_._record      = record_fn

                    def datachange_notification(self_, node, val, data):
                        # Identify variable by resolving browse name asynchronously
                        asyncio.ensure_future(self_._handle(node, val))

                    async def _handle(self_, node, val):
                        bn = await node.read_browse_name()
                        self_._record(self_._agent_key, bn.Name, val)

                handler = LogHandler(
                    agent_key,
                    self.TRACKED_VARIABLES.get(agent_key, []),
                    self._record,
                )
                sub = await c.create_subscription(500, handler)

                nodes_to_watch = []
                for var_name in self.TRACKED_VARIABLES.get(agent_key, []):
                    node = await self._browse_variable(c, var_name)
                    if node:
                        nodes_to_watch.append(node)
                    else:
                        logger.warning(
                            "[DataLogger] Could not find node '%s' on %s",
                            var_name, agent_key,
                        )

                if nodes_to_watch:
                    await sub.subscribe_data_change(nodes_to_watch)
                    self._subscriptions.append(sub)
                    logger.info(
                        "[DataLogger] Watching %d node(s) on agent '%s'",
                        len(nodes_to_watch), agent_key,
                    )

            except Exception as exc:
                logger.error(
                    "[DataLogger] Failed to connect to %s (%s): %s",
                    agent_key, url, exc,
                )

    async def stop(self):
        """Unsubscribe, disconnect, and flush data to CSV."""
        for sub in self._subscriptions:
            try:
                await sub.delete()
            except Exception:
                pass

        for client in self._clients:
            try:
                await client.__aexit__(None, None, None)
            except Exception:
                pass

        self._flush_to_csv()

    def _flush_to_csv(self):
        if not self._rows:
            logger.info("[DataLogger] No data to write.")
            return
        fieldnames = ["timestamp", "agent", "variable", "value"]
        with open(self._output_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self._rows)
        logger.info(
            "[DataLogger] Wrote %d records → %s",
            len(self._rows), os.path.abspath(self._output_file),
        )


# ---------------------------------------------------------------------------
# Storage cross-subscription wiring
# ---------------------------------------------------------------------------

async def wire_storage_subscriptions(storage: StorageAgent):
    """
    Subscribe the StorageAgent to the Producer's CurrentGeneration and the
    Consumer's CurrentPowerDemand so it can compute the power balance.
    """
    def on_generation(node, val, data):
        storage.latest_generation_kw = float(val)

    def on_demand(node, val, data):
        storage.latest_demand_kw = float(val)

    logger.info("[Runner] Wiring storage subscriptions to producer and consumer…")
    await storage.subscribe_to_peer(
        CLIENT_ENDPOINTS["producer"],
        "CurrentGeneration",
        period_ms=1000,
        on_change=on_generation,
    )
    await storage.subscribe_to_peer(
        CLIENT_ENDPOINTS["consumer"],
        "CurrentPowerDemand",
        period_ms=1000,
        on_change=on_demand,
    )
    logger.info("[Runner] Storage subscriptions active.")


# ---------------------------------------------------------------------------
# Main simulation
# ---------------------------------------------------------------------------

async def run_simulation():
    logger.info("=" * 60)
    logger.info("DPC Factory MAS Simulation Starting")
    logger.info("Duration : %d seconds", SIMULATION_DURATION_SECONDS)
    logger.info("Topology : 1 Producer | 1 Consumer | 1 Storage")
    logger.info("=" * 60)

    # ---- Instantiate agents ----
    producer = ProducerAgent(
        agent_id="PV_Array_1",
        endpoint_url=ENDPOINTS["producer"],
        peak_capacity_kw=80.0,
    )
    consumer = ConsumerAgent(
        agent_id="Machine_Line_1",
        endpoint_url=ENDPOINTS["consumer"],
        base_demand_kw=50.0,
        criticality=1,
    )
    storage = StorageAgent(
        agent_id="BESS_1",
        endpoint_url=ENDPOINTS["storage"],
        capacity_kwh=200.0,
        max_charge_rate_kw=40.0,
        initial_soc_pct=50.0,
    )

    data_logger = DataLogger(output_file=LOG_FILE)

    async with producer, consumer, storage:
        logger.info("[Runner] All agents started.")

        # Allow servers a moment to become fully addressable on the network
        await asyncio.sleep(2.0)

        # Wire Storage subscriptions (must happen after all servers are up)
        await wire_storage_subscriptions(storage)

        # Allow initial subscription data to arrive before logger attaches
        await asyncio.sleep(1.0)

        # Start centralized data logger
        await data_logger.start(CLIENT_ENDPOINTS)
        logger.info("[Runner] DataLogger connected to all agents.")

        # ---- Run scenario ----
        logger.info("[Runner] Simulation running for %d seconds…", SIMULATION_DURATION_SECONDS)
        await asyncio.sleep(SIMULATION_DURATION_SECONDS)

        # ---- Shutdown ----
        logger.info("[Runner] Simulation complete. Shutting down…")
        await data_logger.stop()

    logger.info("[Runner] All agents stopped. Results in: %s", LOG_FILE)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(run_simulation())
