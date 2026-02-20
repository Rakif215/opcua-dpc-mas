"""
factory_agents.py
-----------------
Specialized agent classes for the DPC factory MAS.
Each class extends OpcUaAgent (from opcua_agent.py) to add:
  - A richer OPC UA ObjectType (ConsumerAgentType / ProducerAgentType /
    StorageAgentType) with domain-specific variables.
  - An async simulation loop that evolves those variables over time to
    mimic realistic factory physics.

Do NOT modify opcua_agent.py when changing agent behaviours.
"""

import asyncio
import logging
import math
import random

from asyncua import ua

from opcua_agent import OpcUaAgent

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper: add a writable variable and return its node
# ---------------------------------------------------------------------------

async def _add_var(parent, ns, name, value, variant_type):
    node = await parent.add_variable(ns, name, ua.Variant(value, variant_type))
    await node.set_writable(True)
    return node


# ===========================================================================
# ConsumerAgent
# ===========================================================================

class ConsumerAgent(OpcUaAgent):
    """
    Represents an energy-consuming machine or production line.

    Additional OPC UA variables
    ---------------------------
    CurrentPowerDemand : Float   – live consumption in kW
    ForecastedDemand   : Float[] – 8-step ahead demand forecast in kW
    Criticality        : UInt16  – 0=sheddable, 1=important, 2=critical

    Simulation behaviour
    --------------------
    Demand follows a sinusoidal base load with additive Gaussian noise,
    representing a machine cycling through different production phases.
    Forecasted demand is a simple linear projection + small noise.
    """

    def __init__(
        self,
        agent_id: str,
        endpoint_url: str,
        base_demand_kw: float = 50.0,
        criticality: int = 1,
    ):
        super().__init__(agent_id, "Consumer", endpoint_url, initial_state="Active")
        self._base_demand = base_demand_kw
        self._criticality = criticality

        # OPC UA variable nodes (set after start())
        self._node_demand: object = None
        self._node_forecast: object = None
        self._node_criticality: object = None

    async def _build_base_type(self):
        """Extend BaseAgentType with ConsumerAgentType-specific nodes."""
        # Let parent build the base object first
        await super()._build_base_type()
        ns = self._ns_idx
        parent = self._agent_obj   # the instantiated OPC UA object node

        self._node_demand = await _add_var(
            parent, ns, "CurrentPowerDemand",
            self._base_demand, ua.VariantType.Double
        )
        self._node_forecast = await _add_var(
            parent, ns, "ForecastedDemand",
            [self._base_demand] * 8, ua.VariantType.Double
        )
        self._node_criticality = await _add_var(
            parent, ns, "Criticality",
            self._criticality, ua.VariantType.UInt16
        )
        logger.debug("[%s] ConsumerAgentType nodes created.", self.agent_id)

    async def _simulation_loop(self):
        """
        Continuously update CurrentPowerDemand and ForecastedDemand.
        Demand oscillates around a base load with sinusoidal + noise pattern.
        """
        t = 0.0
        await asyncio.sleep(0.5)   # wait for server to be fully ready
        while True:
            try:
                # Sinusoidal production cycle (period ≈ 30 seconds) + noise
                demand = self._base_demand * (
                    0.7 + 0.3 * math.sin(2 * math.pi * t / 30.0)
                ) + random.gauss(0, 2.0)
                demand = max(0.0, demand)

                # 8-step forecast: linear continuation with decreasing certainty
                forecast = [
                    max(0.0, demand + random.gauss(0, 2.0 + i * 0.5))
                    for i in range(8)
                ]

                await self._node_demand.write_value(round(demand, 2))
                await self._node_forecast.write_value(forecast)

                # State transitions based on demand level
                if demand > self._base_demand * 1.2:
                    await self.set_state("HighLoad")
                elif demand < self._base_demand * 0.4:
                    await self.set_state("Idle")
                else:
                    await self.set_state("Active")

                logger.debug("[%s] demand=%.2f kW", self.agent_id, demand)
                t += 1.0
                await asyncio.sleep(1.0)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("[%s] Simulation error: %s", self.agent_id, exc)
                await asyncio.sleep(2.0)


# ===========================================================================
# ProducerAgent
# ===========================================================================

class ProducerAgent(OpcUaAgent):
    """
    Represents a renewable energy source (e.g., PV array or wind turbine).

    Additional OPC UA variables
    ---------------------------
    CurrentGeneration   : Float   – live power output in kW
    ForecastedGeneration: Float[] – 8-step ahead generation forecast in kW
    CurtailmentActive   : Boolean – true when output is being artificially capped

    Simulation behaviour
    --------------------
    Generation follows an idealised solar irradiance curve
    (Gaussian bell centred on a simulated solar noon) with additive noise,
    scaled to a configurable peak capacity.
    """

    def __init__(
        self,
        agent_id: str,
        endpoint_url: str,
        peak_capacity_kw: float = 80.0,
    ):
        super().__init__(agent_id, "Producer", endpoint_url, initial_state="Active")
        self._peak_capacity = peak_capacity_kw
        self._sim_hour = 6.0   # start at 06:00; each sim step = 6 min real time

        self._node_generation: object = None
        self._node_forecast: object = None
        self._node_curtailment: object = None

    async def _build_base_type(self):
        await super()._build_base_type()
        ns = self._ns_idx
        parent = self._agent_obj

        self._node_generation = await _add_var(
            parent, ns, "CurrentGeneration",
            0.0, ua.VariantType.Double
        )
        self._node_forecast = await _add_var(
            parent, ns, "ForecastedGeneration",
            [0.0] * 8, ua.VariantType.Double
        )
        self._node_curtailment = await _add_var(
            parent, ns, "CurtailmentActive",
            False, ua.VariantType.Boolean
        )
        logger.debug("[%s] ProducerAgentType nodes created.", self.agent_id)

    @staticmethod
    def _solar_curve(hour: float, peak: float) -> float:
        """Gaussian solar irradiance centred at 12:00."""
        if hour < 6.0 or hour > 20.0:
            return 0.0
        return peak * math.exp(-0.5 * ((hour - 13.0) / 3.5) ** 2)

    async def _simulation_loop(self):
        """Advance solar generation simulation by 10 minutes per real second."""
        await asyncio.sleep(0.5)
        while True:
            try:
                generation = max(
                    0.0,
                    self._solar_curve(self._sim_hour, self._peak_capacity)
                    + random.gauss(0, 1.5)
                )
                forecast = [
                    max(0.0, self._solar_curve(self._sim_hour + i * (10 / 60), self._peak_capacity)
                        + random.gauss(0, 1.5))
                    for i in range(1, 9)
                ]

                await self._node_generation.write_value(round(generation, 2))
                await self._node_forecast.write_value([round(v, 2) for v in forecast])

                state = "Active" if generation > 0 else "Idle"
                await self.set_state(state)

                logger.debug(
                    "[%s] hour=%.2f generation=%.2f kW", self.agent_id,
                    self._sim_hour, generation
                )

                # Advance simulated clock: 10 min per real second
                self._sim_hour = (self._sim_hour + 10 / 60) % 24
                await asyncio.sleep(1.0)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("[%s] Simulation error: %s", self.agent_id, exc)
                await asyncio.sleep(2.0)


# ===========================================================================
# StorageAgent
# ===========================================================================

class StorageAgent(OpcUaAgent):
    """
    Represents a Battery Energy Storage System (BESS).

    Additional OPC UA variables
    ---------------------------
    StateOfCharge  : Float – current fill level 0–100 %
    MaxChargeRate  : Float – maximum power that can be absorbed (kW)
    Capacity       : Float – total usable capacity (kWh)

    Simulation behaviour
    --------------------
    The storage agent monitors peer Producer and Consumer nodes (via OPC UA
    subscriptions set up by the simulation runner). Internally it decides
    whether to charge (excess generation > demand) or discharge (demand >
    generation), updating StateOfCharge every second accordingly.
    It exposes its current mode via OperationalState:
       "Charging" | "Discharging" | "Idle" | "Full" | "Empty"
    """

    def __init__(
        self,
        agent_id: str,
        endpoint_url: str,
        capacity_kwh: float = 200.0,
        max_charge_rate_kw: float = 40.0,
        initial_soc_pct: float = 50.0,
    ):
        super().__init__(agent_id, "Storage", endpoint_url, initial_state="Idle")
        self._capacity = capacity_kwh
        self._max_charge_rate = max_charge_rate_kw
        self._soc = initial_soc_pct

        self._node_soc: object = None
        self._node_charge_rate: object = None
        self._node_capacity: object = None

        # Shared state updated by external subscriptions (set by runner)
        self.latest_generation_kw: float = 0.0
        self.latest_demand_kw: float = 0.0

    async def _build_base_type(self):
        await super()._build_base_type()
        ns = self._ns_idx
        parent = self._agent_obj

        self._node_soc = await _add_var(
            parent, ns, "StateOfCharge",
            self._soc, ua.VariantType.Double
        )
        self._node_charge_rate = await _add_var(
            parent, ns, "MaxChargeRate",
            self._max_charge_rate, ua.VariantType.Double
        )
        self._node_capacity = await _add_var(
            parent, ns, "Capacity",
            self._capacity, ua.VariantType.Double
        )
        logger.debug("[%s] StorageAgentType nodes created.", self.agent_id)

    async def _simulation_loop(self):
        """
        Every second, compute net power balance and update StateOfCharge.
        Time step is 1 real second which represents a configurable sim interval.
        Here 1 s → 1 minute of factory time for a compact demo.
        """
        sim_minutes_per_step = 1
        await asyncio.sleep(1.0)   # allow subscriptions to populate initial values
        while True:
            try:
                net_kw = self.latest_generation_kw - self.latest_demand_kw

                # Clamp power to battery limits
                charge_kw = max(-self._max_charge_rate,
                                min(self._max_charge_rate, net_kw))

                # ΔSoC = (power × Δt[h]) / capacity [kWh] × 100 %
                delta_h = sim_minutes_per_step / 60.0
                delta_soc = (charge_kw * delta_h / self._capacity) * 100.0
                self._soc = max(0.0, min(100.0, self._soc + delta_soc))

                await self._node_soc.write_value(round(self._soc, 2))

                # Update operational state
                if self._soc >= 99.9:
                    state = "Full"
                elif self._soc <= 0.1:
                    state = "Empty"
                elif charge_kw > 0.5:
                    state = "Charging"
                elif charge_kw < -0.5:
                    state = "Discharging"
                else:
                    state = "Idle"

                await self.set_state(state)

                logger.debug(
                    "[%s] SoC=%.1f%% charge_kw=%.2f state=%s",
                    self.agent_id, self._soc, charge_kw, state,
                )
                await asyncio.sleep(1.0)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("[%s] Simulation error: %s", self.agent_id, exc)
                await asyncio.sleep(2.0)
