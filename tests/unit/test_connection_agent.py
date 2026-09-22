"""ConnectionAgent: secrets → login → master (memory/disk) → status."""
import asyncio

from scapler.agents.connection import ConnectionAgent
from scapler.core.bus import EventBus, Inbox
from scapler.core.config import Settings
from scapler.core.messages import Topic
from scapler.core.secrets_store import SecretStore
from tests._master import make_master


class FakeAdapter:
    name = "fake"

    def __init__(self, master=None, fail_login=False):
        self.master = master
        self.fail_login = fail_login
        self.logged_in = 0
        self.disconnected = 0

    async def login(self, code=""):
        self.logged_in += 1
        if self.fail_login:
            raise RuntimeError("broker rejected credentials")

    async def load_master(self, today=None):
        self.master = make_master()
        return self.master

    async def positions(self):
        return []

    async def disconnect(self):
        self.disconnected += 1


async def _conn(tmp_path, factory):
    bus = EventBus()
    out = Inbox()
    bus.subscribe(out, Topic.CONNECTION_STATUS)
    cfg = Settings(data_dir=str(tmp_path))
    store = SecretStore(tmp_path)
    c = ConnectionAgent(bus, cfg, store, clock_fn=lambda: "10:00",
                        adapter_factory=factory)
    await c.start()
    return bus, out, c, store


async def test_connect_happy_path_and_disk_cache(tmp_path):
    made = []

    def factory(broker, secrets):
        a = FakeAdapter()
        made.append((broker, secrets, a))
        return a

    bus, out, c, store = await _conn(tmp_path, factory)
    await c.save_secrets("groww", {"api_key": "gk", "api_secret": "gs",
                                   "": ""})
    assert store.load("groww") == {"api_key": "gk", "api_secret": "gs"}
    r = await c.connect("groww")
    assert r["ok"] and c.sessions["groww"]["state"] == "CONNECTED"
    assert c.active == "groww"
    assert made[0][1] == {"api_key": "gk", "api_secret": "gs"}
    assert made[0][2].logged_in == 1
    # master came from the network (nothing cached) and is now ON DISK
    assert c.master is not None and len(c.master.options) > 0
    from scapler.brokers.master_cache import load_disk
    assert load_disk(tmp_path, "fake") is not None
    # a second adapter would hit the disk cache (memory→disk→network)
    await c.disconnect("groww")
    assert c.sessions["groww"]["state"] == "DISCONNECTED"
    assert made[0][2].disconnected == 1 and c.active is None
    await c.stop()


async def test_connect_without_credentials_errors(tmp_path):
    bus, out, c, store = await _conn(tmp_path, lambda b, s: FakeAdapter())
    r = await c.connect("upstox")
    assert r["ok"] is False and "missing credentials" in r["error"]
    assert c.sessions["upstox"]["state"] == "ERROR"
    await c.stop()


async def test_login_failure_surfaces_error(tmp_path):
    def factory(broker, secrets):
        return FakeAdapter(fail_login=True)

    bus, out, c, store = await _conn(tmp_path, factory)
    store.save("groww", {"api_key": "gk", "api_secret": "gs"})
    r = await c.connect("groww")
    assert r["ok"] is False and "broker rejected" in r["error"]
    assert c.sessions["groww"]["state"] == "ERROR"
    await c.stop()


async def test_status_published_on_every_transition(tmp_path):
    bus, out, c, store = await _conn(tmp_path, lambda b, s: FakeAdapter())
    await c.connect("groww")                      # ERROR (no creds)
    await asyncio.sleep(0.05)
    topics = []
    while out.depth():
        env = await out.get()
        topics.append(env.payload["state"])
    assert topics == ["ERROR"]
    await c.stop()
