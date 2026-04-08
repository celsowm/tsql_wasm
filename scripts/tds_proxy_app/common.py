from __future__ import annotations

import json
import datetime as dt
import os
import re
import subprocess
import threading
import sys
from dataclasses import dataclass
from pathlib import Path


TDS_PACKET_NAMES = {
    0x01: "SQL_BATCH",
    0x02: "PRE_TDS7_LOGIN",
    0x03: "RPC",
    0x04: "TABULAR_RESULT",
    0x06: "ATTENTION",
    0x10: "LOGIN7",
    0x12: "PRELOGIN",
}

INTNTYPE = 0x26
GUIDTYPE = 0x24
BITNTYPE = 0x68
FLTNTYPE = 0x6D
MONEYNTYPE = 0x6E
DECIMALNTYPE = 0x6A
NUMERICNTYPE = 0x6C
BIGVARCHARTYPE = 0xA7
BIGCHARTYPE = 0xAF
NVARCHARTYPE = 0xE7
NCHARTYPE = 0xEF
BIGBINARYTYPE = 0xAD
BIGVARBINARYTYPE = 0xA5
DATENTYPE = 0x28
TIMENTYPE = 0x29
DATETIME2NTYPE = 0x2A
DATETIMNTYPE = 0x6F


def now_tag() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def ts() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def q(cmd: list[str]) -> str:
    if os.name == "nt":
        return subprocess.list2cmdline(cmd)
    return " ".join(cmd)


def clean(text: str) -> str:
    return "".join(c for c in text if c.isprintable() or c in "\r\n\t")


def supports_ansi_color() -> bool:
    return sys.stdout.isatty() and os.environ.get("NO_COLOR") is None


def ansi_green(text: str) -> str:
    if not supports_ansi_color():
        return text
    return f"\033[32m{text}\033[0m"


@dataclass(frozen=True)
class BackendSpec:
    name: str
    host: str
    port: int


@dataclass(frozen=True)
class SqlCredentials:
    user: str
    password: str


def load_sql_credentials(root: Path | None = None) -> SqlCredentials:
    base = root or repo_root()
    path = base / "scripts" / "credentials.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    if "sql_server_user" not in data or "sql_server_password" not in data:
        raise RuntimeError(f"Missing sql_server_user/sql_server_password in {path}")
    user = str(data["sql_server_user"]).strip()
    password = str(data["sql_server_password"])
    if len(password) < 8:
        raise RuntimeError(
            f"sql_server_password in {path} is too short; SQL Server requires at least 8 characters"
        )
    if not user:
        raise RuntimeError(f"sql_server_user in {path} is empty")
    return SqlCredentials(user=user, password=password)


class RunLogger:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._fh = path.open("a", encoding="utf-8", newline="\n")

    def close(self) -> None:
        with self._lock:
            self._fh.flush()
            self._fh.close()

    def line(self, msg: str) -> None:
        text = f"[{ts()}] {msg}"
        with self._lock:
            self._fh.write(text + "\n")
            self._fh.flush()
        print(text, flush=True)

    def line_console(self, msg: str, console_msg: str) -> None:
        text = f"[{ts()}] {msg}"
        with self._lock:
            self._fh.write(text + "\n")
            self._fh.flush()
        print(console_msg, flush=True)

    def blank(self) -> None:
        with self._lock:
            self._fh.write("\n")
            self._fh.flush()
        print("", flush=True)

    def block(self, prefix: str, text: str) -> None:
        for line in text.splitlines():
            line = line.strip()
            if line:
                self.line(prefix + line)


class PhaseState:
    def __init__(self, backend: BackendSpec) -> None:
        self._lock = threading.Lock()
        self._backend = backend
        self._phase = backend.name

    def get(self) -> tuple[str, BackendSpec]:
        with self._lock:
            return self._phase, self._backend

    def set(self, phase: str, backend: BackendSpec) -> None:
        with self._lock:
            self._phase = phase
            self._backend = backend


class PacketLogger:
    def __init__(self, runlog: RunLogger, phase: PhaseState, conn_id: int) -> None:
        self.runlog = runlog
        self.phase = phase
        self.conn_id = conn_id
        self.buffers = {"C2S": bytearray(), "S2C": bytearray()}

    def feed(self, direction: str, data: bytes) -> None:
        buf = self.buffers[direction]
        buf.extend(data)
        while True:
            if len(buf) < 8:
                return
            pkt_len = int.from_bytes(buf[2:4], "big")
            if pkt_len < 8 or pkt_len > 65535:
                self._emit_raw(direction, bytes(buf))
                buf.clear()
                return
            if len(buf) < pkt_len:
                return
            packet = bytes(buf[:pkt_len])
            del buf[:pkt_len]
            self._emit_packet(direction, packet)

    def flush(self, direction: str) -> None:
        buf = self.buffers[direction]
        if buf:
            self._emit_raw(direction, bytes(buf), partial=True)
            buf.clear()

    def _emit_raw(self, direction: str, data: bytes, partial: bool = False) -> None:
        phase, _ = self.phase.get()
        kind = "partial " if partial else ""
        self.runlog.line(
            f"[{phase}] conn={self.conn_id} {direction} {kind}chunk {len(data)} bytes"
        )

    def _emit_packet(self, direction: str, packet: bytes) -> None:
        phase, _ = self.phase.get()
        pkt_type = packet[0]
        status = packet[1]
        pkt_len = int.from_bytes(packet[2:4], "big")
        name = TDS_PACKET_NAMES.get(pkt_type, f"0x{pkt_type:02X}")
        self.runlog.line(
            f"[{phase}] conn={self.conn_id} {direction} {name} "
            f"type=0x{pkt_type:02X} status=0x{status:02X} len={pkt_len}"
        )
        preview = self._preview(pkt_type, packet[8:])
        if preview:
            self.runlog.line(f"[{phase}] conn={self.conn_id} {direction} decoded: {preview}")

    def _preview(self, pkt_type: int, payload: bytes) -> str:
        if not payload:
            return ""
        if pkt_type == 0x12:
            return self._decode_prelogin(payload)
        if pkt_type == 0x10:
            return self._decode_login7(payload)
        if pkt_type == 0x01:
            return self._extract_sql_text(payload)
        if pkt_type == 0x03:
            return self._extract_sql_text(payload) or self._extract_ascii_summary(payload)
        if pkt_type == 0x04:
            return self._decode_tabular_result(payload)
        return self._extract_sql_text(payload) or self._extract_ascii_summary(payload)

    def _decode_login7(self, payload: bytes) -> str:
        if len(payload) < 94:
            return ""

        def u16(pos: int) -> int:
            return int.from_bytes(payload[pos : pos + 2], "little")

        def u32(pos: int) -> int:
            return int.from_bytes(payload[pos : pos + 4], "little")

        def s32(pos: int) -> int:
            return int.from_bytes(payload[pos : pos + 4], "little", signed=True)

        def utf16_at(offset: int, chars: int) -> str:
            if offset == 0 or chars == 0:
                return ""
            end = offset + chars * 2
            if end > len(payload):
                return ""
            raw = payload[offset:end]
            try:
                return clean(raw.decode("utf-16le", errors="ignore")).strip()
            except Exception:
                return ""

        packet_len = u32(0)
        tds_version = u32(4)
        packet_size = u32(8)
        client_prog_ver = u32(12)
        client_pid = u32(16)
        connection_id = u32(20)
        option_flags1 = payload[24]
        option_flags2 = payload[25]
        type_flags = payload[26]
        option_flags3 = payload[27]
        client_time_zone = s32(28)
        client_lcid = u32(32)

        ib_hostname = u16(36)
        cch_hostname = u16(38)
        ib_username = u16(40)
        cch_username = u16(42)
        ib_password = u16(44)
        cch_password = u16(46)
        ib_app_name = u16(48)
        cch_app_name = u16(50)
        ib_server_name = u16(52)
        cch_server_name = u16(54)
        ib_unused = u16(56)
        cb_unused = u16(58)
        ib_client_int_name = u16(60)
        cch_client_int_name = u16(62)
        ib_language = u16(64)
        cch_language = u16(66)
        ib_database = u16(68)
        cch_database = u16(70)
        ib_client_id = 72
        ib_sspi = u16(78)
        cb_sspi = u16(80)
        ib_attach_db_file = u16(82)
        cch_attach_db_file = u16(84)
        ib_change_password = u16(86)
        cch_change_password = u16(88)
        cb_sspi_long = u32(90)

        hostname = utf16_at(ib_hostname, cch_hostname)
        username = utf16_at(ib_username, cch_username)
        app_name = utf16_at(ib_app_name, cch_app_name)
        server_name = utf16_at(ib_server_name, cch_server_name)
        client_interface_name = utf16_at(ib_client_int_name, cch_client_int_name)
        language = utf16_at(ib_language, cch_language)
        database = utf16_at(ib_database, cch_database)
        attach_db_file = utf16_at(ib_attach_db_file, cch_attach_db_file)
        change_password = utf16_at(ib_change_password, cch_change_password)

        password = ""
        if cch_password > 0 and ib_password + (cch_password * 2) <= len(payload):
            encrypted = payload[ib_password : ib_password + (cch_password * 2)]
            password = self._decode_login7_password(encrypted)

        sspi_len = 0
        if cb_sspi_long:
            sspi_len = cb_sspi_long
        elif cb_sspi:
            sspi_len = cb_sspi
        sspi = ""
        if sspi_len and ib_sspi + sspi_len <= len(payload):
            sspi = f"sspi={sspi_len} bytes"

        parts = [
            f"packet_len={packet_len}",
            f"tds={self._format_tds_version(tds_version)}",
            f"packet_size={packet_size}",
            f"client_prog_ver=0x{client_prog_ver:08X}",
            f"client_pid={client_pid}",
            f"connection_id={connection_id}",
            f"flags1=0x{option_flags1:02X}",
            f"flags2=0x{option_flags2:02X}",
            f"type_flags=0x{type_flags:02X}",
            f"flags3=0x{option_flags3:02X}",
            f"timezone={client_time_zone}",
            f"lcid=0x{client_lcid:08X}",
        ]
        if hostname:
            parts.append(f"host={hostname}")
        if username:
            parts.append(f"user={username}")
        if password:
            parts.append(f"password={password}")
        if app_name:
            parts.append(f"app={app_name}")
        if server_name:
            parts.append(f"server={server_name}")
        if client_interface_name:
            parts.append(f"client_if={client_interface_name}")
        if language:
            parts.append(f"language={language}")
        if database:
            parts.append(f"db={database}")
        if attach_db_file:
            parts.append(f"attach_db={attach_db_file}")
        if change_password:
            parts.append(f"change_pw={change_password}")
        if sspi:
            parts.append(sspi)
        return "LOGIN7 " + ", ".join(parts)

    def _decode_login7_password(self, encrypted: bytes) -> str:
        if len(encrypted) % 2 != 0:
            return ""
        decoded = bytearray()
        for b in encrypted:
            xored = b ^ 0xA5
            decoded.append(((xored & 0x0F) << 4) | ((xored & 0xF0) >> 4))
        try:
            return clean(decoded.decode("utf-16le", errors="ignore")).strip()
        except Exception:
            return ""

    def _format_tds_version(self, value: int) -> str:
        major = (value >> 24) & 0xFF
        minor = (value >> 16) & 0xFF
        build = value & 0xFFFF
        return f"0x{value:08X} ({major}.{minor}.{build})"

    def _extract_sql_text(self, payload: bytes) -> str:
        try:
            text = clean(payload.decode("utf-16le", errors="ignore"))
        except Exception:
            return ""
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            return ""

        upper = text.upper()
        keywords = (
            "SELECT",
            "DECLARE",
            "EXECUTE",
            "EXEC ",
            "USE ",
            "SET ",
            "IF ",
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE",
            "ALTER",
            "DROP",
            "MERGE",
            "WITH ",
        )
        start = min((upper.find(tok) for tok in keywords if upper.find(tok) != -1), default=-1)
        if start != -1:
            return text[start : start + 600]
        if len(payload) < 256:
            return text[:240]
        return ""

    def _extract_ascii_summary(self, payload: bytes) -> str:
        try:
            text = clean(payload.decode("utf-16le", errors="ignore"))
        except Exception:
            return ""
        words = re.findall(r"[A-Za-z0-9_./:\\@#\-\[\]\(\)']{3,}", text)
        if not words:
            return ""
        seen = set()
        compact = []
        for word in words:
            key = word.lower()
            if key in seen:
                continue
            seen.add(key)
            compact.append(word)
            if len(compact) == 8:
                break
        return " | ".join(compact)

    def _decode_prelogin(self, payload: bytes) -> str:
        if len(payload) < 5:
            return ""

        token_names = {
            0x00: "VERSION",
            0x01: "ENCRYPTION",
            0x02: "INSTOPT",
            0x03: "THREADID",
            0x04: "MARS",
            0x05: "TRACEID",
            0x06: "FEDAUTH",
            0x07: "NONCE",
        }
        parts: list[str] = []
        pos = 0
        while pos < len(payload):
            token = payload[pos]
            pos += 1
            if token == 0xFF:
                break
            if pos + 4 > len(payload):
                break
            offset = int.from_bytes(payload[pos : pos + 2], "big")
            length = int.from_bytes(payload[pos + 2 : pos + 4], "big")
            pos += 4
            if offset + length > len(payload):
                continue
            value = payload[offset : offset + length]
            name = token_names.get(token, f"0x{token:02X}")
            parts.append(f"{name}={self._format_prelogin_value(token, value)}")
        return "PRELOGIN " + ", ".join(parts) if parts else ""

    def _format_prelogin_value(self, token: int, value: bytes) -> str:
        if token == 0x01 and value:
            return {
                0x00: "OFF",
                0x01: "ON",
                0x02: "NOT_SUP",
                0x03: "REQ",
            }.get(value[0], f"0x{value[0]:02X}")
        if token == 0x02:
            return value.split(b"\x00", 1)[0].decode("ascii", errors="ignore") or "empty"
        if token == 0x03 and len(value) == 4:
            return str(int.from_bytes(value, "big"))
        if token == 0x00 and len(value) >= 6:
            major = value[0]
            minor = value[1]
            build = int.from_bytes(value[2:4], "big")
            return f"{major}.{minor}.{build}"
        return value.hex()

    def _decode_tabular_result(self, payload: bytes) -> str:
        tokens = self._parse_tabular_tokens(payload)
        if not tokens:
            return ""

        parts: list[str] = []
        for token in tokens:
            kind = token["kind"]
            if kind == "COLMETADATA":
                cols = token.get("columns", [])
                parts.append("columns=" + ", ".join(cols) if cols else "columns=0")
            elif kind == "ROW":
                values = token.get("values", [])
                if values:
                    parts.append(f"row[{token.get('index', 0)}]=" + ", ".join(values[:6]))
                else:
                    parts.append(f"row[{token.get('index', 0)}]")
            elif kind == "DONE":
                status = token.get("status", 0)
                row_count = token.get("row_count")
                row_text = f", rows={row_count}" if row_count is not None else ""
                parts.append(f"DONE status=0x{status:04X}{row_text}")
            elif kind == "ERROR":
                parts.append(f"ERROR number={token.get('number', 0)} state={token.get('state', 0)}")
            elif kind == "INFO":
                parts.append(f"INFO number={token.get('number', 0)} state={token.get('state', 0)}")

        return " | ".join(parts[:6]) if parts else ""

    def _parse_tabular_tokens(self, payload: bytes) -> list[dict[str, object]]:
        tokens: list[dict[str, object]] = []
        pos = 0
        current_columns: list[dict[str, object]] = []
        row_index = 0

        while pos < len(payload):
            token = payload[pos]
            pos += 1

            if token == 0x81:
                if pos + 2 > len(payload):
                    break
                count = int.from_bytes(payload[pos : pos + 2], "little")
                pos += 2
                columns: list[dict[str, object]] = []
                for _ in range(count):
                    if pos + 7 > len(payload):
                        break
                    pos += 4
                    pos += 2
                    tds_type = payload[pos]
                    pos += 1
                    pos = self._skip_type_info(payload, pos, tds_type)
                    if pos >= len(payload):
                        break
                    name_len = payload[pos]
                    pos += 1
                    name_bytes = payload[pos : pos + name_len * 2]
                    pos += name_len * 2
                    try:
                        name = clean(name_bytes.decode("utf-16le", errors="ignore")).strip()
                    except Exception:
                        name = ""
                    columns.append({"name": name, "type": tds_type})
                current_columns = columns
                tokens.append(
                    {
                        "kind": "COLMETADATA",
                        "columns": [c["name"] or f"col{idx + 1}" for idx, c in enumerate(columns)],
                    }
                )
                continue

            if token == 0xD1:
                row_index += 1
                values, pos = self._decode_row(payload, pos, current_columns)
                tokens.append({"kind": "ROW", "index": row_index, "values": values})
                continue

            if token in (0xFD, 0xFE, 0xFF):
                if pos + 12 > len(payload):
                    break
                status = int.from_bytes(payload[pos : pos + 2], "little")
                pos += 2
                pos += 2
                row_count = int.from_bytes(payload[pos : pos + 8], "little")
                pos += 8
                tokens.append({"kind": "DONE", "status": status, "row_count": row_count})
                continue

            if token in (0xAA, 0xAB):
                if pos + 2 > len(payload):
                    break
                length = int.from_bytes(payload[pos : pos + 2], "little")
                pos += 2
                data = payload[pos : pos + length]
                pos += length
                tokens.append(self._parse_error_info(token, data))
                continue

            break

        return tokens

    def _skip_type_info(self, payload: bytes, pos: int, tds_type: int) -> int:
        if tds_type in (INTNTYPE, BITNTYPE, FLTNTYPE, MONEYNTYPE, DATENTYPE, TIMENTYPE, DATETIME2NTYPE, DATETIMNTYPE, GUIDTYPE):
            if pos >= len(payload):
                return len(payload)
            length = payload[pos]
            pos += 1 + length
            if tds_type in (TIMENTYPE, DATETIME2NTYPE):
                pos += 1
            return min(len(payload), pos)
        if tds_type in (DECIMALNTYPE, NUMERICNTYPE):
            if pos + 3 > len(payload):
                return len(payload)
            pos += 1  # length
            pos += 1  # precision
            pos += 1  # scale
            return min(len(payload), pos)
        if tds_type in (BIGVARCHARTYPE, BIGCHARTYPE, NVARCHARTYPE, NCHARTYPE, BIGBINARYTYPE, BIGVARBINARYTYPE):
            if pos + 2 > len(payload):
                return len(payload)
            pos += 2
            if tds_type in (BIGVARCHARTYPE, BIGCHARTYPE, NVARCHARTYPE, NCHARTYPE):
                pos += 5
            return min(len(payload), pos)
        return len(payload)

    def _decode_row(
        self,
        payload: bytes,
        pos: int,
        columns: list[dict[str, object]],
    ) -> tuple[list[str], int]:
        values: list[str] = []
        for col in columns:
            tds_type = int(col.get("type", 0))
            name = str(col.get("name") or f"col{len(values) + 1}")
            value, pos = self._decode_row_value(payload, pos, tds_type)
            values.append(f"{name}={value}")
        return values, pos

    def _decode_row_value(self, payload: bytes, pos: int, tds_type: int) -> tuple[str, int]:
        if pos >= len(payload):
            return "<truncated>", len(payload)

        if tds_type == INTNTYPE:
            if pos >= len(payload):
                return "<truncated>", len(payload)
            length = payload[pos]
            pos += 1
            raw = payload[pos : pos + length]
            pos += length
            if not raw:
                return "NULL", pos
            value = int.from_bytes(raw.ljust(8, b"\x00"), "little", signed=True)
            return str(value), pos

        if tds_type == BITNTYPE:
            if pos + 2 > len(payload):
                return "<truncated>", len(payload)
            length = payload[pos]
            pos += 1
            raw = payload[pos : pos + length]
            pos += length
            if not raw:
                return "NULL", pos
            return "1" if raw[0] != 0 else "0", pos

        if tds_type == FLTNTYPE:
            if pos >= len(payload):
                return "<truncated>", len(payload)
            length = payload[pos]
            pos += 1
            raw = payload[pos : pos + length]
            pos += length
            if len(raw) == 4:
                import struct

                return str(struct.unpack("<f", raw)[0]), pos
            if len(raw) == 8:
                import struct

                return str(struct.unpack("<d", raw)[0]), pos
            return raw.hex(), pos

        if tds_type == MONEYNTYPE:
            if pos >= len(payload):
                return "<truncated>", len(payload)
            length = payload[pos]
            pos += 1
            raw = payload[pos : pos + length]
            pos += length
            if len(raw) == 4:
                v = int.from_bytes(raw, "little", signed=True)
                return f"{v / 10000:.4f}", pos
            if len(raw) == 8:
                v = int.from_bytes(raw, "little", signed=True)
                return f"{v / 10000:.4f}", pos
            return raw.hex(), pos

        if tds_type in (BIGVARCHARTYPE, BIGCHARTYPE):
            if pos + 2 > len(payload):
                return "<truncated>", len(payload)
            length = int.from_bytes(payload[pos : pos + 2], "little")
            pos += 2
            raw = payload[pos : pos + length]
            pos += length
            try:
                return clean(raw.decode("latin1", errors="ignore")).strip(), pos
            except Exception:
                return raw.hex(), pos

        if tds_type in (NVARCHARTYPE, NCHARTYPE):
            if pos + 2 > len(payload):
                return "<truncated>", len(payload)
            length = int.from_bytes(payload[pos : pos + 2], "little")
            pos += 2
            raw = payload[pos : pos + length]
            pos += length
            try:
                return clean(raw.decode("utf-16le", errors="ignore")).strip(), pos
            except Exception:
                return raw.hex(), pos

        if tds_type in (BIGVARBINARYTYPE, BIGBINARYTYPE):
            if pos + 2 > len(payload):
                return "<truncated>", len(payload)
            length = int.from_bytes(payload[pos : pos + 2], "little")
            pos += 2
            raw = payload[pos : pos + length]
            pos += length
            return "0x" + raw.hex(), pos

        if tds_type == DATENTYPE:
            if pos >= len(payload):
                return "<truncated>", len(payload)
            length = payload[pos]
            pos += 1
            raw = payload[pos : pos + length]
            pos += length
            if len(raw) != 3:
                return raw.hex(), pos
            days = int.from_bytes(raw, "little", signed=True)
            return self._format_date_from_2000(days), pos

        if tds_type == TIMENTYPE:
            if pos >= len(payload):
                return "<truncated>", len(payload)
            length = payload[pos]
            pos += 1
            raw = payload[pos : pos + length]
            pos += length
            if not raw:
                return "NULL", pos
            ticks = int.from_bytes(raw.ljust(8, b"\x00"), "little")
            return self._format_time_from_ticks(ticks), pos

        if tds_type == DATETIMNTYPE:
            if pos >= len(payload):
                return "<truncated>", len(payload)
            length = payload[pos]
            pos += 1
            raw = payload[pos : pos + length]
            pos += length
            if len(raw) != 8:
                return raw.hex(), pos
            days = int.from_bytes(raw[:4], "little", signed=True)
            ticks = int.from_bytes(raw[4:], "little")
            return f"{self._format_date_from_2000(days)} {self._format_time_from_300hz_ticks(ticks)}", pos

        if tds_type == DATETIME2NTYPE:
            if pos >= len(payload):
                return "<truncated>", len(payload)
            length = payload[pos]
            pos += 1
            raw = payload[pos : pos + length]
            pos += length
            if len(raw) != 8:
                return raw.hex(), pos
            ticks = int.from_bytes(raw[:5], "little")
            days = int.from_bytes(raw[5:], "little", signed=True)
            return f"{self._format_date_from_2000(days)} {self._format_time_from_ticks(ticks)}", pos

        if tds_type == GUIDTYPE:
            if pos >= len(payload):
                return "<truncated>", len(payload)
            length = payload[pos]
            pos += 1
            raw = payload[pos : pos + length]
            pos += length
            if len(raw) != 16:
                return raw.hex(), pos
            data1 = int.from_bytes(raw[0:4], "little")
            data2 = int.from_bytes(raw[4:6], "little")
            data3 = int.from_bytes(raw[6:8], "little")
            data4 = raw[8:]
            return (
                f"{data1:08x}-{data2:04x}-{data3:04x}-"
                f"{data4[0]:02x}{data4[1]:02x}-"
                f"{data4[2]:02x}{data4[3]:02x}{data4[4]:02x}{data4[5]:02x}{data4[6]:02x}{data4[7]:02x}"
            ), pos

        if tds_type in (DECIMALNTYPE, NUMERICNTYPE):
            if pos + 3 > len(payload):
                return "<truncated>", len(payload)
            length = payload[pos]
            precision = payload[pos + 1]
            scale = payload[pos + 2]
            pos += 3
            raw = payload[pos : pos + length]
            pos += length
            if not raw:
                return "NULL", pos
            sign = raw[0]
            magnitude = int.from_bytes(raw[1:], "little")
            value = magnitude / (10 ** scale)
            if sign == 0:
                value = -value
            return f"{value:.{scale}f}", pos

        return self._decode_generic_value(payload, pos)

    def _decode_generic_value(self, payload: bytes, pos: int) -> tuple[str, int]:
        if pos >= len(payload):
            return "<truncated>", len(payload)
        length = payload[pos]
        pos += 1
        raw = payload[pos : pos + length]
        pos += length
        return "0x" + raw.hex(), pos

    def _format_date_from_2000(self, days: int) -> str:
        base = dt.datetime(2000, 1, 1)
        try:
            value = base + dt.timedelta(days=days)
            return value.strftime("%Y-%m-%d")
        except Exception:
            return f"days={days}"

    def _format_time_from_ticks(self, ticks: int) -> str:
        if ticks == 0:
            return "00:00:00.0000000"
        seconds, frac_ticks = divmod(ticks, 10_000_000)
        hours, rem = divmod(seconds, 3600)
        minutes, secs = divmod(rem, 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}.{frac_ticks:07d}"

    def _format_time_from_300hz_ticks(self, ticks: int) -> str:
        if ticks == 0:
            return "00:00:00.000"
        total_seconds = ticks / 300.0
        whole_seconds = int(total_seconds)
        millis = int(round((total_seconds - whole_seconds) * 1000))
        hours, rem = divmod(whole_seconds, 3600)
        minutes, secs = divmod(rem, 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}.{millis:03d}"

    def _parse_error_info(self, token: int, data: bytes) -> dict[str, object]:
        if len(data) < 6:
            return {"kind": "INFO" if token == 0xAB else "ERROR"}
        number = int.from_bytes(data[0:4], "little", signed=True)
        state = data[4]
        class_ = data[5]
        pos = 6

        def read_us_vchar_utf16() -> str:
            nonlocal pos
            if pos + 2 > len(data):
                return ""
            chars = int.from_bytes(data[pos : pos + 2], "little")
            pos += 2
            nbytes = chars * 2
            if pos + nbytes > len(data):
                return ""
            raw = data[pos : pos + nbytes]
            pos += nbytes
            return clean(raw.decode("utf-16le", errors="ignore")).strip()

        message = read_us_vchar_utf16()
        server_name = read_us_vchar_utf16()
        proc_name = read_us_vchar_utf16()
        line_number = int.from_bytes(data[pos : pos + 4], "little", signed=True) if pos + 4 <= len(data) else 0

        return {
            "kind": "INFO" if token == 0xAB else "ERROR",
            "number": number,
            "state": state,
            "class": class_,
            "message": message,
            "server_name": server_name,
            "proc_name": proc_name,
            "line_number": line_number,
        }
