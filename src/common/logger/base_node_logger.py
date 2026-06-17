import os
import glob
import json
import logging
import zlib
from typing import Tuple, Set, Optional, Dict, Any, List

logger = logging.getLogger(__name__)

class BaseNodeLogger:
    def __init__(self, base_filepath: str):
        self.base_filepath = base_filepath
        self.eof_filepath = f"{base_filepath}_eof.log"
        self.eof_done_filepath = f"{base_filepath}_eof_done.log"
        self.checkpoint_done_filepath = f"{base_filepath}_checkpoint_done.log"
        self.batch_state_filepath = f"{base_filepath}_batch_state.json"

        if not os.path.exists(self.eof_filepath):
            open(self.eof_filepath, 'ab').close()
        if not os.path.exists(self.eof_done_filepath):
            open(self.eof_done_filepath, 'ab').close()
        if not os.path.exists(self.checkpoint_done_filepath):
            open(self.checkpoint_done_filepath, 'ab').close()

        self._eof_fd = open(self.eof_filepath, 'ab')
        self._eof_done_fd = open(self.eof_done_filepath, 'ab')
        self._checkpoint_done_fd = open(self.checkpoint_done_filepath, 'ab')
        self._buffer_fds: Dict[Tuple[str, str], Any] = {}

    def _write_in_file(self, fd, data: Dict[str, Any], sync: bool = True):
        payload = json.dumps(data).encode('utf-8')
        longitud = len(payload).to_bytes(4, "big")
        checksum = zlib.crc32(payload).to_bytes(4, "big")

        fd.write(longitud)
        fd.write(payload)
        fd.write(checksum)
        fd.flush()
        if sync:
            os.fsync(fd.fileno())

    def _read_from_file(self, filepath: str):
        if not os.path.exists(filepath):
            return

        with open(filepath, 'rb') as f:
            while True:
                len_bytes = f.read(4)
                if not len_bytes or len(len_bytes) < 4:
                    break
                
                longitud = int.from_bytes(len_bytes, "big")
                payload = f.read(longitud)
                if len(payload) < longitud:
                    break

                checksum_bytes = f.read(4)
                if len(checksum_bytes) < 4:
                    break
                
                expected_checksum = zlib.crc32(payload).to_bytes(4, "big")
                if checksum_bytes != expected_checksum:
                    break

                try:
                    yield json.loads(payload.decode('utf-8'))
                except json.JSONDecodeError:
                    break

    def save_batch_state(self, batch_id: Optional[str], index: int, last_completed: Optional[str] = None):
        tmp_path = self.batch_state_filepath + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump({"batch_id": batch_id, "index": index, "last_completed": last_completed}, f)
        os.replace(tmp_path, self.batch_state_filepath)

    def recover_batch_state(self) -> Tuple[Optional[str], int, Optional[str]]:
        if not os.path.exists(self.batch_state_filepath):
            return None, 0, None
        try:
            with open(self.batch_state_filepath, "r") as f:
                state = json.load(f)
                return state.get("batch_id"), state.get("index", 0), state.get("last_completed")
        except (json.JSONDecodeError, IOError):
            return None, 0, None

    def log_eof(self, client_id: Optional[str], emitter_id: str):
        self._write_in_file(self._eof_fd, {
            "client_id": client_id, 
            "emitter_id": emitter_id
        }, sync=True)

    def recover_eofs(self) -> Tuple[Set[str], Dict[str, Set[str]]]:
        eof_global = set()
        eof_clients = {}
        for record in self._read_from_file(self.eof_filepath):
            cid = record.get("client_id")
            eid = record.get("emitter_id")
            if cid is None:
                eof_global.add(eid)
            else:
                eof_clients.setdefault(cid, set()).add(eid)
        return eof_global, eof_clients

    def clear_eof(self, client_id: Optional[str]):
        records = [
            record
            for record in self._read_from_file(self.eof_filepath)
            if record.get("client_id") != client_id
        ]

        tmp_path = self.eof_filepath + ".tmp"
        with open(tmp_path, "wb") as tmp_fd:
            for record in records:
                self._write_in_file(tmp_fd, record, sync=False)
            tmp_fd.flush()
            os.fsync(tmp_fd.fileno())
        os.replace(tmp_path, self.eof_filepath)

        if self._eof_fd and not self._eof_fd.closed:
            self._eof_fd.close()
        self._eof_fd = open(self.eof_filepath, 'ab')


    def _eof_done_key(self, client_id: Optional[str]) -> str:
        return "__global__" if client_id is None else str(client_id)

    def log_eof_done(self, client_id: Optional[str]):
        self.log_eof_done_key(self._eof_done_key(client_id), client_id)

    def log_eof_done_key(self, key: str, client_id: Optional[str] = None):
        self._write_in_file(self._eof_done_fd, {
            "client_id": client_id,
            "key": key,
        }, sync=True)

    def recover_eof_done(self) -> Set[str]:
        completed = set()
        for record in self._read_from_file(self.eof_done_filepath):
            key = record.get("key")
            if key is None:
                key = self._eof_done_key(record.get("client_id"))
            completed.add(key)
        return completed

    def clear_eof_done(self, client_id: Optional[str]):
        self.clear_eof_done_key(self._eof_done_key(client_id))

    def clear_eof_done_key(self, key_to_clear: str):
        records = [
            record
            for record in self._read_from_file(self.eof_done_filepath)
            if (record.get("key") or self._eof_done_key(record.get("client_id"))) != key_to_clear
        ]

        tmp_path = self.eof_done_filepath + ".tmp"
        with open(tmp_path, "wb") as tmp_fd:
            for record in records:
                self._write_in_file(tmp_fd, record, sync=False)
            tmp_fd.flush()
            os.fsync(tmp_fd.fileno())
        os.replace(tmp_path, self.eof_done_filepath)

        if self._eof_done_fd and not self._eof_done_fd.closed:
            self._eof_done_fd.close()
        self._eof_done_fd = open(self.eof_done_filepath, 'ab')

    def log_checkpoint_done_key(self, key: str, client_id: Optional[str] = None, checkpoint_id: Optional[str] = None):
        self._write_in_file(self._checkpoint_done_fd, {
            "client_id": client_id,
            "checkpoint_id": checkpoint_id,
            "key": key,
        }, sync=True)

    def recover_checkpoint_done(self) -> Set[str]:
        completed = set()
        for record in self._read_from_file(self.checkpoint_done_filepath):
            key = record.get("key")
            if key is not None:
                completed.add(key)
        return completed

    def clear_checkpoint_done_for_client(self, client_id: Optional[str]):
        client_key = None if client_id is None else str(client_id)
        records = [
            record
            for record in self._read_from_file(self.checkpoint_done_filepath)
            if (None if record.get("client_id") is None else str(record.get("client_id"))) != client_key
        ]

        tmp_path = self.checkpoint_done_filepath + ".tmp"
        with open(tmp_path, "wb") as tmp_fd:
            for record in records:
                self._write_in_file(tmp_fd, record, sync=False)
            tmp_fd.flush()
            os.fsync(tmp_fd.fileno())
        os.replace(tmp_path, self.checkpoint_done_filepath)

        if self._checkpoint_done_fd and not self._checkpoint_done_fd.closed:
            self._checkpoint_done_fd.close()
        self._checkpoint_done_fd = open(self.checkpoint_done_filepath, 'ab')

    def _get_buffer_filepath(self, client_id: Optional[str], buf_key: str) -> str:
        safe_cid = client_id if client_id is not None else "global"
        return f"{self.base_filepath}_buf_{safe_cid}_{buf_key}.log"

    def append_to_buffer(self, client_id: Optional[str], buf_key: str, data: dict):
        key = (client_id, buf_key)
        if key not in self._buffer_fds:
            filepath = self._get_buffer_filepath(client_id, buf_key)
            self._buffer_fds[key] = open(filepath, 'ab')

        self._write_in_file(self._buffer_fds[key], data, sync=False)

    def clear_buffer(self, client_id: Optional[str], buf_key: str):
        key = (client_id, buf_key)
        if key in self._buffer_fds:
            self._buffer_fds[key].close()
            del self._buffer_fds[key]
        
        filepath = self._get_buffer_filepath(client_id, buf_key)
        if os.path.exists(filepath):
            os.remove(filepath)

    def load_all_buffers(self) -> Dict[Tuple[Optional[str], str], List[dict]]:
        buffers = {}
        pattern = f"{self.base_filepath}_buf_*.log"
        
        for filepath in glob.glob(pattern):
            try:
                basename = filepath.replace(f"{self.base_filepath}_buf_", "")
                basename = basename[:-4] # Quitar .log
                parts = basename.split("_", 1)
                
                if len(parts) == 2:
                    client_id, buf_key = parts
                    client_id = None if client_id == "global" else client_id
                    key = (client_id, buf_key)
                    
                    buffers[key] = []
                    for record in self._read_from_file(filepath):
                        buffers[key].append(record)
            except Exception as e:
                logger.error(f"Error cargando el buffer {filepath}: {e}")
                
        return buffers

    def close(self):
        if self._eof_fd and not self._eof_fd.closed:
            self._eof_fd.close()
        if self._eof_done_fd and not self._eof_done_fd.closed:
            self._eof_done_fd.close()
        if self._checkpoint_done_fd and not self._checkpoint_done_fd.closed:
            self._checkpoint_done_fd.close()
        for fd in self._buffer_fds.values():
            if not fd.closed:
                fd.close()
