#!/usr/bin/env python3
"""Optional independent ABI check against the system libtraceevent reader."""
import ctypes
import ctypes.util
import struct
from pathlib import Path

lib = ctypes.CDLL(ctypes.util.find_library("traceevent"))
lib.kbuffer_alloc.argtypes = [ctypes.c_int, ctypes.c_int]
lib.kbuffer_alloc.restype = ctypes.c_void_p
lib.kbuffer_load_subbuffer.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
lib.kbuffer_read_event.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_ulonglong)]
lib.kbuffer_read_event.restype = ctypes.c_void_p
lib.kbuffer_next_event.argtypes = lib.kbuffer_read_event.argtypes
lib.kbuffer_next_event.restype = ctypes.c_void_p
lib.kbuffer_event_size.argtypes = [ctypes.c_void_p]
lib.kbuffer_missed_events.argtypes = [ctypes.c_void_p]
lib.kbuffer_free.argtypes = [ctypes.c_void_p]
fixtures = Path(__file__).parent / "fixtures" / "offline"

for name, word, endian, prefix, lengths in [
    ("little64.page", 8, 1, "<", [64, 36, 64, 64]),
    ("big32.page", 4, 0, ">", [60, 36, 60, 60]),
]:
    page = ctypes.create_string_buffer((fixtures / name).read_bytes())
    buffer = lib.kbuffer_alloc(1 if word == 8 else 0, endian)
    assert buffer
    assert lib.kbuffer_load_subbuffer(buffer, page) == 0
    assert lib.kbuffer_missed_events(buffer) == 9
    timestamp = ctypes.c_ulonglong()
    event = lib.kbuffer_read_event(buffer, ctypes.byref(timestamp))
    actual = []
    while event:
        size = lib.kbuffer_event_size(buffer)
        event_id = struct.unpack(prefix + "H", ctypes.string_at(event, 2))[0]
        actual.append((timestamp.value, event_id, size))
        event = lib.kbuffer_next_event(buffer, ctypes.byref(timestamp))
    assert actual == list(zip([1010, 134218748, 268437467, 268437474],
                              [100, 101, 100, 100], lengths)), actual
    lib.kbuffer_free(buffer)
    print(name, actual, "lost=9")
