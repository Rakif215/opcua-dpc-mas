"""
opcua_agent.py
--------------
Core OPC UA communication layer for the Distributed Production Factory (DPC)
Multi-Agent System. This module is responsible ONLY for communication concerns:
  - Server-side: exposing an OPC UA endpoint and defining BaseAgentType.
  - Client-side: connecting to peers and subscribing to their data nodes.

Depends on: asyncua (pip install asyncua)
"""

import asyncio
import logging
from asyncua import ua, Server, Client

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Subscription Callback Handler
# ---------------------------------------------------------------------------

class AgentSubHandler:
    """
    OPC UA subscription event handler (duck-typed).
    asyncua calls datachange_notification / event_notification on this object
    whenever a monitored node changes. No base class is required – asyncua
    uses duck typing for handler objects.
    """

    def __init__(self, agent_id: str, on_change_callback=None):
        """
        Parameters
        ----------
        agent_id: str
            The identifier of the *subscribing* agent (used for logging).
        on_change_callback: callable | None
            Optional coroutine or regular callable invoked on every value change.
            Signature: callback(node, val, data) → None
        """
        self.agent_id = agent_id
        self._callback = on_change_callback

    def datachange_notification(self, node, val, data):
        logger.info(
            "[%s] DATA-CHANGE subscription event | node=%s | new_value=%s",
            self.agent_id, node, val,
        )
        if self._callback is not None:
            if asyncio.iscoroutinefunction(self._callback):
                asyncio.ensure_future(self._callback(node, val, data))
            else:
                self._callback(node, val, data)

    def event_notification(self, event):
        logger.info("[%s] EVENT notification: %s", self.agent_id, event)


# ---------------------------------------------------------------------------
# Base OPC UA Agent
# ---------------------------------------------------------------------------

class OpcUaAgent:
    """
    Base class for every agent in the DPC factory MAS.

    Responsibilities
    ----------------
    * Server side
        - Instantiate an asyncua.Server on a dedicated endpoint.
        - Register a factory namespace.
        - Define the ``BaseAgentType`` ObjectType and instantiate one object
          (this agent) inside the server address space.
        - Expose ``AgentID``, ``AgentRole``, and ``OperationalState`` variables.

    * Client / PubSub side
        - Provide ``subscribe_to_peer()`` to subscribe to any variable on a
          remote agent server and receive push-based data-change notifications.
        - Manage open client connections for clean shutdown.

    Lifecycle
    ---------
    Use as an async context manager:

        async with OpcUaAgent(...) as agent:
            ...   # agent is running; subscriptions are active

    Or call ``start()`` / ``stop()`` explicitly.
    """

    NAMESPACE_URI: str = "urn:dpc-factory:mas"

    def __init__(
        self,
        agent_id: str,
        agent_role: str,
        endpoint_url: str,
        initial_state: str = "Idle",
    ):
        """
        Parameters
        ----------
        agent_id:     Unique agent name, e.g. "Consumer_1".
        agent_role:   Role label, e.g. "Consumer" | "Producer" | "Storage".
        endpoint_url: The OPC UA server URL this agent listens on,
                      e.g. "opc.tcp://0.0.0.0:4840/dpc/consumer1".
        initial_state: OperationalState string on startup.
        """
        self.agent_id = agent_id
        self.agent_role = agent_role
        self.endpoint_url = endpoint_url
        self.initial_state = initial_state

        # asyncua internals
        self._server: Server = None
        self._ns_idx: int = None

        # Base type nodes (available after start())
        self._node_agent_id: object = None
        self._node_role: object = None
        self._node_state: object = None

        # Track peer subscriptions so we can clean up on stop
        self._peer_clients: list[Client] = []
        self._peer_subscriptions: list = []

        # Background simulation task (overridden by subclasses)
        self._sim_task: asyncio.Task = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self):
        """Start the OPC UA server and expose this agent's information model."""
        self._server = Server()
        await self._server.init()
        self._server.set_endpoint(self.endpoint_url)
        self._server.set_server_name(f"DPC-MAS – {self.agent_id}")

        # Register factory namespace
        self._ns_idx = await self._server.register_namespace(self.NAMESPACE_URI)

        # Build BaseAgentType and instantiate this agent's object
        await self._build_base_type()

        # Start the server
        await self._server.start()
        logger.info("[%s] Server started at %s", self.agent_id, self.endpoint_url)

        # Start behavior loop (subclass responsibility)
        self._sim_task = asyncio.create_task(self._simulation_loop())

    async def stop(self):
        """Gracefully stop subscriptions, client connections, and the server."""
        # Cancel simulation
        if self._sim_task and not self._sim_task.done():
            self._sim_task.cancel()
            try:
                await self._sim_task
            except asyncio.CancelledError:
                pass

        # Close peer subscriptions
        for sub in self._peer_subscriptions:
            try:
                await sub.delete()
            except Exception:
                pass

        # Disconnect peer clients
        for client in self._peer_clients:
            try:
                await client.__aexit__(None, None, None)
            except Exception:
                pass

        # Stop server
        if self._server:
            await self._server.stop()
        logger.info("[%s] Agent shut down.", self.agent_id)

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        await self.stop()

    # ------------------------------------------------------------------
    # Server-side: Information Model
    # ------------------------------------------------------------------

    async def _build_base_type(self):
        """
        Create a plain OPC UA Object for this agent and add all mandatory
        variables directly to the instance.

        Note: asyncua requires a ModellingRule on type-level variables for them
        to be cloned into instances. For prototype simplicity we skip the type
        hierarchy and add variables directly on the object node — semantically
        equivalent and immediately writable without extra setup.
        """
        root = self._server.get_objects_node()
        ns = self._ns_idx

        # Create the agent's container object (no custom ObjectType required)
        agent_obj = await root.add_object(ns, self.agent_id)

        # Add BaseAgentType variables directly on the instance
        self._node_agent_id = await agent_obj.add_variable(
            ns, "AgentID", ua.Variant(self.agent_id, ua.VariantType.String)
        )
        self._node_role = await agent_obj.add_variable(
            ns, "AgentRole", ua.Variant(self.agent_role, ua.VariantType.String)
        )
        self._node_state = await agent_obj.add_variable(
            ns, "OperationalState", ua.Variant(self.initial_state, ua.VariantType.String)
        )

        # Make writable so simulation loops and external callers can update them
        await self._node_agent_id.set_writable(True)
        await self._node_role.set_writable(True)
        await self._node_state.set_writable(True)

        # Store object reference for subclasses to attach more variables
        self._agent_obj = agent_obj

        logger.debug("[%s] Agent object created. ns_idx=%d", self.agent_id, ns)

    # ------------------------------------------------------------------
    # Server-side: State helpers
    # ------------------------------------------------------------------

    async def set_state(self, state: str):
        """Update OperationalState on the server; triggers subscriptions on peers."""
        if self._node_state:
            await self._node_state.write_value(state)
            logger.debug("[%s] OperationalState → %s", self.agent_id, state)

    async def get_state(self) -> str:
        if self._node_state:
            return await self._node_state.read_value()
        return "Unknown"

    # ------------------------------------------------------------------
    # Client-side: PubSub subscription to peers
    # ------------------------------------------------------------------

    async def subscribe_to_peer(
        self,
        peer_url: str,
        variable_browse_name: str,
        period_ms: int = 500,
        on_change=None,
    ):
        """
        Connect to a peer agent's OPC UA server and subscribe to one of its
        variables by browse name.

        Parameters
        ----------
        peer_url:              OPC UA endpoint of the peer, e.g. "opc.tcp://...".
        variable_browse_name:  Name of the variable to monitor, e.g. "OperationalState".
        period_ms:             Publishing interval in milliseconds.
        on_change:             Optional callback(node, val, data). Can be async.

        Returns
        -------
        The active asyncua subscription object.
        """
        client = Client(peer_url)
        await client.__aenter__()
        self._peer_clients.append(client)

        # Discover the target node by browsing the peer's address space
        ns = await client.get_namespace_index(self.NAMESPACE_URI)
        root = client.get_objects_node()
        children = await root.get_children()

        target_node = None
        for obj in children:
            obj_children = await obj.get_children()
            for var in obj_children:
                bn = await var.read_browse_name()
                if bn.Name == variable_browse_name:
                    target_node = var
                    break
            if target_node:
                break

        if target_node is None:
            raise ValueError(
                f"[{self.agent_id}] Variable '{variable_browse_name}' not found "
                f"on peer {peer_url}"
            )

        handler = AgentSubHandler(agent_id=self.agent_id, on_change_callback=on_change)
        sub = await client.create_subscription(period_ms, handler)
        await sub.subscribe_data_change(target_node)
        self._peer_subscriptions.append(sub)

        logger.info(
            "[%s] Subscribed to '%s' on peer %s",
            self.agent_id, variable_browse_name, peer_url,
        )
        return sub

    # ------------------------------------------------------------------
    # Simulation loop (override in subclasses)
    # ------------------------------------------------------------------

    async def _simulation_loop(self):
        """
        Background coroutine executing the agent's behaviour.
        Base class does nothing; specialized agents override this method
        to provide physics simulation and decision logic.
        """
        pass
