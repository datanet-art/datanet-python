"""DataNet Python SDK — async client with sync convenience wrapper.

Protocol summary
----------------
1. Exchange API key for a JWT::

       POST https://api.datanet.art/auth/token
       {"apiKey": "ak_..."}  →  {"token": "<jwt>"}

2. Open a WebSocket with the JWT embedded as a subprotocol::

       wss://ws.datanet.art/ws
       Sec-WebSocket-Protocol: bearer, <jwt>

3. Send / receive JSON envelopes::

       {"op": "sub",   "ch": "project.123.sensor"}
       {"op": "unsub", "ch": "project.123.sensor"}
       {"op": "pub",   "ch": "project.123.sensor", "d": {...}}
       {"op": "hb"}

4. Incoming pub messages::

       {"op": "pub", "ch": "...", "d": ..., "from": "...", "ts": 1234567890}

5. Heartbeat every 30 s keeps the connection alive.
6. Exponential-backoff reconnect: 1 s → 2 s → 4 s … capped at 30 s, up to
   ``max_reconnect_attempts`` total.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Coroutine

import aiohttp
import websockets
from websockets import State
from websockets.exceptions import ConnectionClosed

__all__ = [
    "AnyMessage",
    "BinaryMessageMeta",
    "DataNet",
    "DataNetError",
    "MessageMeta",
    "base64_to_binary",
    "binary_to_base64",
    "build_art_dmx_packet",
    "build_dmx_frame",
]

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Public types
# ──────────────────────────────────────────────────────────────────────────────

#: Type alias for an async message handler.
Handler = Callable[..., Coroutine[Any, Any, None]]

#: Type alias for an async event handler (connect / disconnect / error).
EventHandler = Callable[..., Coroutine[Any, Any, None]]

#: Bytes-like values accepted by binary publish helpers.
BinaryData = bytes | bytearray | memoryview

_HEARTBEAT_INTERVAL = 30  # seconds
_RECONNECT_BASE = 1  # seconds
_RECONNECT_CAP = 30  # seconds


@dataclass
class MessageMeta:
    """Metadata attached to every incoming ``pub`` message."""

    channel: str
    from_: str
    timestamp: int


@dataclass
class BinaryMessageMeta:
    """Metadata attached to incoming binary messages."""

    channel: str
    from_: str
    timestamp: int
    content_type: str
    bytes: int
    metadata: dict[str, Any] | None = None


@dataclass
class AnyMessage:
    """Message wrapper used by ``subscribe_any`` handlers."""

    kind: str
    data: Any
    meta: MessageMeta | BinaryMessageMeta


class DataNetError(RuntimeError):
    """Structured error returned by the DataNet API or gateway."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "gateway_error",
        channel: str | None = None,
        retry_ms: int | None = None,
        scope: str | None = None,
        status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.channel = channel
        self.retry_ms = retry_ms
        self.scope = scope
        self.status = status


def _to_bytes(data: BinaryData) -> bytes:
    if isinstance(data, bytes):
        return data
    if isinstance(data, bytearray):
        return bytes(data)
    if isinstance(data, memoryview):
        return data.tobytes()
    raise TypeError("binary data must be bytes, bytearray, or memoryview")


def binary_to_base64(data: BinaryData) -> str:
    """Encode bytes-like data as a base64 ASCII string."""
    return base64.b64encode(_to_bytes(data)).decode("ascii")


def base64_to_binary(encoded: str) -> bytes:
    """Decode a base64 ASCII string into bytes."""
    return base64.b64decode(encoded.encode("ascii"))


def _clamp_byte(value: int | float) -> int:
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, min(255, numeric))


def _clamp_range(value: int | float, minimum: int, maximum: int) -> int:
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return minimum
    return max(minimum, min(maximum, numeric))


def build_dmx_frame(values: list[int] | tuple[int, ...], length: int = 512) -> bytes:
    """Build a 1-512 byte DMX frame from channel values."""
    frame_length = _clamp_range(length, 1, 512)
    frame = bytearray(frame_length)
    for index, value in enumerate(values[:frame_length]):
        frame[index] = _clamp_byte(value)
    return bytes(frame)


def build_art_dmx_packet(
    dmx: BinaryData | list[int] | tuple[int, ...],
    *,
    universe: int = 0,
    subnet: int = 0,
    net: int = 0,
    sequence: int = 0,
    physical: int = 0,
) -> bytes:
    """Build an Art-Net ArtDMX packet from DMX bytes or channel values."""
    if isinstance(dmx, bytes | bytearray | memoryview):
        dmx_bytes = _to_bytes(dmx)
    else:
        dmx_bytes = build_dmx_frame(list(dmx), min(max(len(dmx), 2), 512))

    frame_length = _clamp_range(max(len(dmx_bytes), 2), 2, 512)
    packet = bytearray(18 + frame_length)
    packet[0:8] = b"Art-Net\x00"
    packet[8] = 0x00
    packet[9] = 0x50
    packet[10] = 0x00
    packet[11] = 14
    packet[12] = _clamp_byte(sequence)
    packet[13] = _clamp_byte(physical)

    port_address = (_clamp_range(subnet, 0, 15) << 4) | _clamp_range(universe, 0, 15)
    packet[14] = port_address & 0xFF
    packet[15] = _clamp_range(net, 0, 127) & 0x7F
    packet[16] = (frame_length >> 8) & 0xFF
    packet[17] = frame_length & 0xFF
    packet[18:] = dmx_bytes[:frame_length]
    return bytes(packet)


# ──────────────────────────────────────────────────────────────────────────────
# Main client
# ──────────────────────────────────────────────────────────────────────────────


class DataNet:
    """Async WebSocket client for the DataNet platform.

    Parameters
    ----------
    api_key:
        Your DataNet API key (``ak_...``).
    device_id:
        Stable device identifier used for presence/history metadata.
    client_id:
        Optional client/app identifier for connection tracking.
    device_name:
        Optional device display name for dashboards/admin tools.
    api_url:
        Base URL of the HTTP REST API.
    ws_url:
        WebSocket endpoint URL.
    max_reconnect_attempts:
        Maximum number of consecutive reconnect attempts before giving up.
        Set to ``0`` for unlimited reconnect attempts.

    Examples
    --------
    Async usage::

        async with DataNet("ak_...") as dn:
            async def on_message(data, meta):
                print(meta.channel, data)

            dn.subscribe("project.abc.sensor", on_message)
            await asyncio.sleep(60)

    Sync usage in a background thread::

        dn = DataNet("ak_...")
        dn.connect_sync()          # blocks until connected, runs in bg thread

        @dn.on("connect")
        async def handle_connect():
            dn.subscribe("project.abc.sensor", my_handler)

        time.sleep(60)
        dn.disconnect()
    """

    def __init__(
        self,
        api_key: str,
        device_id: str | None = None,
        client_id: str | None = None,
        device_name: str | None = None,
        api_url: str = "https://api.datanet.art",
        ws_url: str = "wss://ws.datanet.art",
        max_reconnect_attempts: int = 5,
    ) -> None:
        self._api_key = api_key
        self._device_id = device_id
        self._client_id = client_id
        self._device_name = device_name
        self._api_url = api_url.rstrip("/")
        self._ws_url = ws_url.rstrip("/")
        self._max_reconnect_attempts = max_reconnect_attempts

        # Internal state
        self._jwt: str | None = None
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

        # Subscription registry: channel -> list of handlers
        self._subscribers: dict[str, list[Handler]] = defaultdict(list)
        self._binary_subscribers: dict[str, list[Handler]] = defaultdict(list)
        self._any_subscribers: dict[str, list[Handler]] = defaultdict(list)
        self._binary_content_types: dict[str, str] = {}
        # Currently active subscriptions (channels we've sent SUB for)
        self._active_subs: set[str] = set()

        # Event hooks: "connect" | "disconnect" | "error"
        self._event_handlers: dict[str, list[EventHandler]] = defaultdict(list)

        # Asyncio tasks
        self._recv_task: asyncio.Task[None] | None = None
        self._hb_task: asyncio.Task[None] | None = None
        self._run_task: asyncio.Task[None] | None = None

        # Reconnect bookkeeping
        self._reconnect_attempt = 0
        self._should_run = False

        # For sync usage
        self._thread: threading.Thread | None = None
        self._connected_event = threading.Event()

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def connected(self) -> bool:
        """``True`` when the WebSocket connection is open."""
        if self._ws is None:
            return False
        closed = getattr(self._ws, "closed", None)
        if isinstance(closed, bool):
            return not closed
        state = getattr(self._ws, "state", None)
        return state == State.OPEN

    # ── Public async API ──────────────────────────────────────────────────────

    async def connect(self) -> None:
        """Fetch a JWT and open a WebSocket connection.

        This coroutine returns as soon as the connection is established.
        Call :meth:`disconnect` to close it.  For long-running programs,
        drive the connection from the internal run-loop via
        :meth:`connect_sync` or :meth:`__aenter__`.

        Raises
        ------
        RuntimeError
            If authentication fails or the server rejects the connection.
        aiohttp.ClientError
            On HTTP transport errors during JWT exchange.
        """
        self._loop = asyncio.get_running_loop()
        self._should_run = True
        self._run_task = asyncio.create_task(self._run_loop(), name="datanet-run")

        # Wait until actually connected (or the task fails)
        await self._wait_for_connection()

    async def disconnect(self) -> None:
        """Close the WebSocket connection and stop the run loop gracefully."""
        self._should_run = False
        await self._cancel_tasks()
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        await self._emit("disconnect")

    def subscribe(self, channel: str, handler: Handler) -> None:
        """Register *handler* to be called when a message arrives on *channel*.

        The handler signature must be::

            async def handler(data: Any, meta: MessageMeta) -> None: ...

        If the WebSocket is already connected the SUB envelope is sent
        immediately; otherwise it will be replayed on the next connection.

        Parameters
        ----------
        channel:
            Channel name, e.g. ``"project.abc123.sensor"``.
        handler:
            Async callable invoked with ``(data, meta)`` for each message.
        """
        self._subscribers[channel].append(handler)
        if self.connected and channel not in self._active_subs:
            asyncio.ensure_future(self._send_sub(channel), loop=self._loop)

    def unsubscribe(self, channel: str, handler: Handler | None = None) -> None:
        """Remove *handler* from *channel*, or all handlers if *handler* is ``None``.

        Sends an UNSUB envelope only when the last handler for the channel
        is removed.

        Parameters
        ----------
        channel:
            Channel to unsubscribe from.
        handler:
            Specific handler to remove, or ``None`` to remove all.
        """
        if handler is None:
            self._subscribers.pop(channel, None)
            should_unsub = True
        else:
            handlers = self._subscribers.get(channel, [])
            try:
                handlers.remove(handler)
            except ValueError:
                pass
            if not handlers:
                self._subscribers.pop(channel, None)
            should_unsub = channel not in self._subscribers

        if (
            should_unsub
            and not self._has_handlers(channel)
            and self.connected
            and channel in self._active_subs
        ):
            asyncio.ensure_future(self._send_unsub(channel), loop=self._loop)

    async def publish(
        self,
        channel: str,
        data: Any,
        *,
        content_type: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Publish *data* to *channel*.

        Parameters
        ----------
        channel:
            Target channel.
        data:
            Any JSON-serializable value, or bytes-like data for binary publish.
        content_type:
            Binary content type when *data* is bytes-like.
        metadata:
            Optional binary metadata when *data* is bytes-like.

        Raises
        ------
        RuntimeError
            If not connected.
        TypeError
            If *data* is not JSON-serializable.
        """
        if isinstance(data, bytes | bytearray | memoryview):
            await self.publish_binary(
                channel,
                data,
                content_type=content_type or "application/octet-stream",
                metadata=metadata,
            )
            return

        if not self.connected:
            raise RuntimeError("Not connected — call connect() first.")
        await self._send({"op": "pub", "ch": channel, "d": data})

    async def publish_binary(
        self,
        channel: str,
        data: BinaryData,
        *,
        content_type: str = "application/octet-stream",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Publish bytes to a binary channel with content type and metadata."""
        if not self.connected:
            raise RuntimeError("Not connected — call connect() first.")
        await self._send(
            {
                "op": "pub",
                "ch": channel,
                "bin": True,
                "b64": binary_to_base64(data),
                "ct": content_type,
                "meta": metadata,
            }
        )

    async def publish_dmx(
        self,
        channel: str,
        values: list[int] | tuple[int, ...],
        *,
        length: int = 512,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Clamp values into a DMX frame and publish as ``binary/dmx``."""
        await self.publish_binary(
            channel,
            build_dmx_frame(values, length),
            content_type="binary/dmx",
            metadata=metadata,
        )

    async def publish_artnet(
        self,
        channel: str,
        dmx: BinaryData | list[int] | tuple[int, ...],
        *,
        universe: int = 0,
        subnet: int = 0,
        net: int = 0,
        sequence: int = 0,
        physical: int = 0,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Build an ArtDMX packet and publish as ``binary/artnet``."""
        await self.publish_binary(
            channel,
            build_art_dmx_packet(
                dmx,
                universe=universe,
                subnet=subnet,
                net=net,
                sequence=sequence,
                physical=physical,
            ),
            content_type="binary/artnet",
            metadata=metadata,
        )

    def subscribe_binary(
        self,
        channel: str,
        handler: Handler,
        *,
        content_type: str | None = None,
    ) -> None:
        """Register a handler for binary messages on *channel*."""
        self._binary_subscribers[channel].append(handler)
        if content_type:
            self._binary_content_types[channel] = content_type
        if self.connected and channel not in self._active_subs:
            asyncio.ensure_future(self._send_sub(channel), loop=self._loop)

    def unsubscribe_binary(self, channel: str, handler: Handler | None = None) -> None:
        """Remove a binary handler, or all binary handlers for *channel*."""
        self._remove_handler(self._binary_subscribers, channel, handler)
        if channel not in self._binary_subscribers:
            self._binary_content_types.pop(channel, None)
        if not self._has_handlers(channel) and self.connected and channel in self._active_subs:
            asyncio.ensure_future(self._send_unsub(channel), loop=self._loop)

    def subscribe_any(self, channel: str, handler: Handler) -> None:
        """Register a handler for both JSON and binary messages on *channel*."""
        self._any_subscribers[channel].append(handler)
        if self.connected and channel not in self._active_subs:
            asyncio.ensure_future(self._send_sub(channel), loop=self._loop)

    def unsubscribe_any(self, channel: str, handler: Handler | None = None) -> None:
        """Remove a mixed-message handler, or all mixed handlers for *channel*."""
        self._remove_handler(self._any_subscribers, channel, handler)
        if not self._has_handlers(channel) and self.connected and channel in self._active_subs:
            asyncio.ensure_future(self._send_unsub(channel), loop=self._loop)

    def on(self, event: str, handler: EventHandler | None = None) -> Any:
        """Register an event handler.

        Can be used as a decorator or called directly::

            @dn.on("connect")
            async def handle_connect():
                print("connected!")

            # or
            dn.on("connect", handle_connect)

        Supported events:

        ``"connect"``
            Called with no arguments after each successful connection.
        ``"disconnect"``
            Called with no arguments when the connection is lost.
        ``"error"``
            Called with ``(exception)`` when an error occurs.

        Parameters
        ----------
        event:
            One of ``"connect"``, ``"disconnect"``, ``"error"``.
        handler:
            Async callable, or ``None`` when used as a decorator.
        """
        if handler is None:
            # Decorator usage: @dn.on("connect")
            def decorator(fn: EventHandler) -> EventHandler:
                self._event_handlers[event].append(fn)
                return fn

            return decorator
        self._event_handlers[event].append(handler)
        return handler

    def off(self, event: str, handler: EventHandler) -> None:
        """Remove a previously registered event handler.

        Parameters
        ----------
        event:
            One of ``"connect"``, ``"disconnect"``, ``"error"``.
        handler:
            The handler to remove (must be the same object passed to :meth:`on`).
        """
        handlers = self._event_handlers.get(event, [])
        try:
            handlers.remove(handler)
        except ValueError:
            pass

    # ── Sync convenience wrapper ───────────────────────────────────────────────

    def connect_sync(self, timeout: float = 10.0) -> None:
        """Connect in a background thread and block until connected.

        This is a convenience method for scripts and simple programs that do
        not run their own event loop.  After this call returns the WebSocket
        is open and you can call :meth:`subscribe` / :meth:`publish` from
        any thread.

        Parameters
        ----------
        timeout:
            Seconds to wait for the connection to be established before
            raising :class:`TimeoutError`.

        Raises
        ------
        TimeoutError
            If the connection is not established within *timeout* seconds.
        RuntimeError
            If the background thread fails to start.
        """
        self._connected_event.clear()

        def _thread_target() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
            self._should_run = True

            async def _run() -> None:
                self._run_task = asyncio.create_task(
                    self._run_loop(), name="datanet-run"
                )
                await self._run_task

            try:
                loop.run_until_complete(_run())
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.error("DataNet background thread error: %s", exc)
            finally:
                loop.close()

        self._thread = threading.Thread(
            target=_thread_target, daemon=True, name="datanet-thread"
        )
        self._thread.start()

        if not self._connected_event.wait(timeout=timeout):
            raise TimeoutError(
                f"DataNet: could not connect within {timeout}s."
            )

    # ── Async context manager ─────────────────────────────────────────────────

    async def __aenter__(self) -> "DataNet":
        await self.connect()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.disconnect()

    # ── Internal: run loop (reconnect logic) ──────────────────────────────────

    async def _run_loop(self) -> None:
        """Drive the WebSocket lifecycle with exponential-backoff reconnection."""
        self._reconnect_attempt = 0

        while self._should_run:
            try:
                await self._connect_once()
                # Successful connection: reset counter
                self._reconnect_attempt = 0
            except asyncio.CancelledError:
                break
            except Exception as exc:
                await self._emit("error", exc)
                logger.warning("DataNet connection error: %s", exc)

            if not self._should_run:
                break

            if self._max_reconnect_attempts and (
                self._reconnect_attempt >= self._max_reconnect_attempts
            ):
                logger.error(
                    "DataNet: max reconnect attempts (%d) reached. Giving up.",
                    self._max_reconnect_attempts,
                )
                break

            delay = min(
                _RECONNECT_BASE * (2**self._reconnect_attempt), _RECONNECT_CAP
            )
            self._reconnect_attempt += 1
            logger.info(
                "DataNet: reconnecting in %.1fs (attempt %d/%s)…",
                delay,
                self._reconnect_attempt,
                self._max_reconnect_attempts or "∞",
            )
            await asyncio.sleep(delay)

    async def _connect_once(self) -> None:
        """Fetch a JWT, open the WebSocket, and run until the connection closes."""
        self._jwt = await self._fetch_jwt()

        ws_uri = f"{self._ws_url}/ws"
        logger.debug("DataNet: connecting to %s", ws_uri)
        async with websockets.connect(
            ws_uri,
            subprotocols=["bearer", self._jwt],
            open_timeout=10,
            close_timeout=5,
            ping_interval=None,  # We manage our own heartbeats
        ) as ws:
            self._ws = ws
            logger.info("DataNet: connected.")
            # Signal sync callers that we're up
            self._connected_event.set()

            # Replay subscriptions
            await self._resubscribe_all()

            await self._emit("connect")

            self._hb_task = asyncio.create_task(
                self._heartbeat_loop(), name="datanet-hb"
            )
            try:
                await self._recv_loop()
            finally:
                if self._hb_task and not self._hb_task.done():
                    self._hb_task.cancel()
                    try:
                        await self._hb_task
                    except asyncio.CancelledError:
                        pass

            self._ws = None
            self._active_subs.clear()
            await self._emit("disconnect")

    # ── Internal: JWT ─────────────────────────────────────────────────────────

    async def _fetch_jwt(self) -> str:
        """Exchange the API key for a short-lived JWT."""
        url = f"{self._api_url}/auth/token"
        logger.debug("DataNet: fetching JWT from %s", url)
        payload = {"apiKey": self._api_key}
        if self._device_id:
            payload["deviceId"] = self._device_id
        if self._client_id:
            payload["clientId"] = self._client_id
        if self._device_name:
            payload["deviceName"] = self._device_name
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    code = "authentication_failed"
                    detail = text
                    try:
                        body = json.loads(text)
                        if isinstance(body, dict):
                            code = str(body.get("code") or body.get("error") or code)
                            detail = str(body.get("error") or text)
                    except json.JSONDecodeError:
                        pass
                    raise DataNetError(
                        f"DataNet auth failed ({resp.status}): {detail}",
                        code=code,
                        status=resp.status,
                    )
                payload = await resp.json()
                token = payload.get("token")
                if not token:
                    raise RuntimeError(
                        f"DataNet auth failed: missing token in response {payload!r}"
                    )
                logger.debug("DataNet: JWT acquired.")
                return token

    # ── Internal: receive loop ────────────────────────────────────────────────

    async def _recv_loop(self) -> None:
        """Receive messages from the WebSocket until the connection closes."""
        assert self._ws is not None
        try:
            async for raw in self._ws:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                await self._handle_message(raw)
        except ConnectionClosed as exc:
            logger.info("DataNet: connection closed: %s", exc)

    async def _handle_message(self, raw: str) -> None:
        """Parse and dispatch a raw message string."""
        try:
            envelope = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.warning("DataNet: received non-JSON message: %s — %s", raw, exc)
            return

        op: str = envelope.get("op", "")
        if envelope.get("type") == "error" and envelope.get("error"):
            error = DataNetError(
                f"DataNet: {envelope.get('error')}",
                code=str(envelope.get("code") or envelope.get("error")),
                channel=envelope.get("channel") or envelope.get("ch"),
                retry_ms=envelope.get("retry_ms"),
                scope=envelope.get("scope"),
            )
            await self._emit("error", error)
            return

        if op != "pub":
            # We only dispatch pub messages to handlers; ignore acks etc.
            logger.debug("DataNet: received op=%s (ignored)", op)
            return

        if envelope.get("bin") is True and isinstance(envelope.get("b64"), str):
            await self._handle_binary_envelope(envelope)
            return

        channel: str = envelope.get("ch", "")
        data: Any = envelope.get("d")
        meta = MessageMeta(
            channel=channel,
            from_=envelope.get("from", ""),
            timestamp=envelope.get("ts", 0),
        )

        handlers = list(self._subscribers.get(channel, []))
        any_handlers = list(self._any_subscribers.get(channel, []))
        if not handlers and not any_handlers:
            logger.debug("DataNet: unhandled message on channel %s", channel)
            return

        for handler in handlers:
            try:
                await handler(data, meta)
            except Exception as exc:
                logger.exception(
                    "DataNet: handler error on channel %s: %s", channel, exc
                )
                await self._emit("error", exc)

        if any_handlers:
            message = AnyMessage(kind="json", data=data, meta=meta)
            for handler in any_handlers:
                try:
                    await handler(message)
                except Exception as exc:
                    logger.exception(
                        "DataNet: any handler error on channel %s: %s", channel, exc
                    )
                    await self._emit("error", exc)

    async def _handle_binary_envelope(self, envelope: dict[str, Any]) -> None:
        channel: str = envelope.get("ch", "")
        encoded = envelope.get("b64")
        if not isinstance(encoded, str):
            return

        try:
            data = base64_to_binary(encoded)
        except Exception as exc:
            logger.warning("DataNet: invalid binary payload on %s: %s", channel, exc)
            await self._emit("error", exc)
            return

        meta_value = envelope.get("meta")
        metadata = meta_value if isinstance(meta_value, dict) else None
        meta = BinaryMessageMeta(
            channel=channel,
            from_=envelope.get("from", ""),
            timestamp=envelope.get("ts", 0),
            content_type=(
                envelope.get("ct")
                or self._binary_content_types.get(channel)
                or "application/octet-stream"
            ),
            bytes=int(envelope.get("bytes") or len(data)),
            metadata=metadata,
        )

        handlers = list(self._binary_subscribers.get(channel, []))
        any_handlers = list(self._any_subscribers.get(channel, []))
        if not handlers and not any_handlers:
            logger.debug("DataNet: unhandled binary message on channel %s", channel)
            return

        for handler in handlers:
            try:
                await handler(data, meta)
            except Exception as exc:
                logger.exception(
                    "DataNet: binary handler error on channel %s: %s", channel, exc
                )
                await self._emit("error", exc)

        if any_handlers:
            message = AnyMessage(kind="binary", data=data, meta=meta)
            for handler in any_handlers:
                try:
                    await handler(message)
                except Exception as exc:
                    logger.exception(
                        "DataNet: any handler error on channel %s: %s", channel, exc
                    )
                    await self._emit("error", exc)

    # ── Internal: heartbeat ───────────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        """Send a heartbeat envelope every ``_HEARTBEAT_INTERVAL`` seconds."""
        while True:
            await asyncio.sleep(_HEARTBEAT_INTERVAL)
            if not self.connected:
                break
            try:
                await self._send({"op": "hb"})
                logger.debug("DataNet: heartbeat sent.")
            except Exception as exc:
                logger.warning("DataNet: heartbeat failed: %s", exc)
                break

    # ── Internal: send helpers ────────────────────────────────────────────────

    async def _send(self, envelope: dict[str, Any]) -> None:
        """Serialize *envelope* and send it over the WebSocket."""
        if not self.connected:
            raise RuntimeError("DataNet: WebSocket is not connected.")
        await self._ws.send(json.dumps(envelope))

    async def _send_sub(self, channel: str) -> None:
        await self._send({"op": "sub", "ch": channel})
        self._active_subs.add(channel)
        logger.debug("DataNet: subscribed to %s", channel)

    async def _send_unsub(self, channel: str) -> None:
        await self._send({"op": "unsub", "ch": channel})
        self._active_subs.discard(channel)
        logger.debug("DataNet: unsubscribed from %s", channel)

    async def _resubscribe_all(self) -> None:
        """Re-send SUB for every registered channel after a reconnect."""
        channels = set(self._subscribers)
        channels.update(self._binary_subscribers)
        channels.update(self._any_subscribers)
        for channel in sorted(channels):
            try:
                await self._send_sub(channel)
            except Exception as exc:
                logger.warning(
                    "DataNet: failed to resubscribe to %s: %s", channel, exc
                )

    def _has_handlers(self, channel: str) -> bool:
        return (
            channel in self._subscribers
            or channel in self._binary_subscribers
            or channel in self._any_subscribers
        )

    @staticmethod
    def _remove_handler(
        registry: dict[str, list[Handler]],
        channel: str,
        handler: Handler | None,
    ) -> None:
        if handler is None:
            registry.pop(channel, None)
            return

        handlers = registry.get(channel, [])
        try:
            handlers.remove(handler)
        except ValueError:
            pass
        if not handlers:
            registry.pop(channel, None)

    # ── Internal: events ─────────────────────────────────────────────────────

    async def _emit(self, event: str, *args: Any) -> None:
        """Call all registered handlers for *event*."""
        for handler in list(self._event_handlers.get(event, [])):
            try:
                await handler(*args)
            except Exception as exc:
                logger.exception(
                    "DataNet: event handler error for '%s': %s", event, exc
                )

    # ── Internal: task cleanup ────────────────────────────────────────────────

    async def _cancel_tasks(self) -> None:
        """Cancel all background asyncio tasks."""
        for task in (self._run_task, self._recv_task, self._hb_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._run_task = None
        self._recv_task = None
        self._hb_task = None

    async def _wait_for_connection(self, timeout: float = 10.0) -> None:
        """Await until the WebSocket is open or the run task fails."""
        deadline = time.monotonic() + timeout
        while not self.connected:
            if self._run_task and self._run_task.done():
                exc = self._run_task.exception()
                if exc:
                    raise exc
                return
            if time.monotonic() > deadline:
                raise TimeoutError(f"DataNet: did not connect within {timeout}s.")
            await asyncio.sleep(0.05)
