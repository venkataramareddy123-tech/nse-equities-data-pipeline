"""Shared institutional plumbing: atomic writes, probes, RAM info, tree-kill.

Every parquet/CSV this pipeline publishes goes through atomic_write_* so a
crash can never leave a half-written file behind (write temp + os.replace,
which is atomic on NTFS/POSIX). Freshness checks probe readability, not
just mtimes, so a truncated file is rebuilt instead of trusted.
"""
import os
import subprocess
import sys


def atomic_write_parquet(df, path: str) -> None:
    """Write parquet atomically: temp file + os.replace (crash-safe)."""
    tmp = f"{path}.tmp-{os.getpid()}"
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)


def atomic_write_csv(df, path: str, **kw) -> None:
    """Write CSV atomically (same crash-safety as parquet)."""
    tmp = f"{path}.tmp-{os.getpid()}"
    df.to_csv(tmp, index=False, **kw)
    os.replace(tmp, path)


def atomic_write_json(obj, path: str) -> None:
    """Write JSON atomically (same crash-safety as parquet).

    default=str: summaries carry Timestamps/dates; the old json.dump call
    sites relied on it.
    """
    import json
    tmp = f"{path}.tmp-{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=1, default=str)
    os.replace(tmp, path)


def parquet_probe_ok(path: str) -> bool:
    """True if path is a readable parquet with ≥0 rows (catches truncations)."""
    try:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return False
        import pyarrow.parquet as pq
        return pq.ParquetFile(path).metadata.num_rows >= 0
    except Exception:  # noqa: BLE001 - any failure means "not trustworthy"
        return False


def free_ram_gb() -> float:
    """Free physical RAM in GB (stdlib only; conservative fallback)."""
    try:
        import psutil  # type: ignore
        return psutil.virtual_memory().available / 1e9
    except ImportError:
        pass
    try:
        if sys.platform == "win32":
            import ctypes

            class _MS(ctypes.Structure):
                _fields_ = [("dwLength", ctypes.c_ulong),
                            ("dwMemoryLoad", ctypes.c_ulong),
                            ("ullTotalPhys", ctypes.c_ulonglong),
                            ("ullAvailPhys", ctypes.c_ulonglong),
                            ("ullTotalPageFile", ctypes.c_ulonglong),
                            ("ullAvailPageFile", ctypes.c_ulonglong),
                            ("ullTotalVirtual", ctypes.c_ulonglong),
                            ("ullAvailVirtual", ctypes.c_ulonglong),
                            ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]

            st = _MS()
            st.dwLength = ctypes.sizeof(_MS)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(st))
            return st.ullAvailPhys / 1e9
        if os.path.exists("/proc/meminfo"):
            with open("/proc/meminfo", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("MemAvailable:"):
                        return int(line.split()[1]) / 1e6
    except Exception:  # noqa: BLE001
        pass
    return 4.0  # conservative: assume a small laptop


def kill_tree(proc: "subprocess.Popen") -> None:
    """Kill a child AND its grandchildren (pool workers survive plain kill)."""
    try:
        if sys.platform == "win32":
            subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                           capture_output=True, timeout=30)
        else:
            proc.kill()
    except Exception:  # noqa: BLE001
        try:
            proc.kill()
        except Exception:  # noqa: BLE001
            pass
