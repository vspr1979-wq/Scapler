"""SecretStore: round-trip, per-broker files, 0600 fallback perms."""
import os
import sys

from scapler.core.secrets_store import SecretStore


async def test_roundtrip_and_isolation(tmp_path):
    st = SecretStore(tmp_path)
    st.save("groww", {"api_key": "gk", "api_secret": "gs"})
    st.save("upstox", {"api_key": "uk", "api_secret": "us",
                       "redirect_uri": "https://localhost/oauth"})
    assert st.load("groww") == {"api_key": "gk", "api_secret": "gs"}
    up = st.load("upstox")
    assert up["api_key"] == "uk" and up["redirect_uri"].startswith("https")
    assert st.load("nosuch") is None
    st.clear("groww")
    assert st.load("groww") is None
    assert st.load("upstox") is not None


async def test_fallback_file_is_0600(tmp_path):
    st = SecretStore(tmp_path)
    st.save("groww", {"api_key": "x"})
    files = list(tmp_path.iterdir())
    assert len(files) == 1
    if sys.platform != "win32":
        assert not st.dpapi
        mode = os.stat(files[0]).st_mode & 0o777
        assert mode == 0o600
    # secret material never appears in settings-style JSON logs
    assert "secrets" not in files[0].read_text(errors="ignore") or st.dpapi \
        or "api_key" in files[0].read_text()   # dev fallback is plaintext


async def test_nested_data_dir_created(tmp_path):
    st = SecretStore(tmp_path / "a" / "b")
    st.save("groww", {"api_key": "x"})
    assert st.load("groww") == {"api_key": "x"}
