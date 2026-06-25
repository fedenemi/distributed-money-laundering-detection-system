import heapq
import os

from pathlib import Path

MAX_ELEMENTS_IN_RAM = 10000
DATA_FILE = "data.txt"
TEMP_DATA_FILE = "temp_data.txt"



class DiskSet():

    def __init__(self, id=None, encode_func=None, decode_func=None, base_dir=None):
        # Define RAM set
        self._internal_set = set()

        # Store serialization functions
        self._encode_func = encode_func
        self._decode_func = decode_func

        # Define folder
        storage_base = Path(base_dir or "/tmp")
        if id is not None:
            self._data_dir = storage_base.joinpath(f"storage_{id}")
        else:
            self._data_dir = storage_base.joinpath("storage")
        os.makedirs(self._data_dir, exist_ok=True)

        # Define file
        self._data_file_path = self._data_dir.joinpath(DATA_FILE)
        if not self._data_file_path.exists():
            self._data_file_path.touch()
        self._temp_data_file_path = self._data_dir.joinpath(TEMP_DATA_FILE)


    def cleanup(self):
        self._internal_set.clear()
        self._data_file_path.unlink(missing_ok=True)
        self._temp_data_file_path.unlink(missing_ok=True)
        try:
            self._data_dir.rmdir()
        except:
            pass


    # Internal functions
    def _add_lf_to_strings(self, elements):
        for elem in elements:
            yield elem + '\n'


    def _apply_encode_function(self, elem):
        return elem if self._encode_func is None else self._encode_func(elem)
    

    def _apply_decode_function(self, elem):
        if elem.endswith('\n'):
            elem = elem[:-1]
        return elem if self._decode_func is None else self._decode_func(elem)


    def _flush_set(self):
        # Check if there are elements
        if len(self._internal_set) == 0:
            return

        # Order set
        ordered_set = list(self._internal_set)
        ordered_set.sort()

        # Open files and merge
        with open(self._data_file_path, "r") as current_data, open(self._temp_data_file_path, "w") as new_data:
            files_merger = heapq.merge(current_data, self._add_lf_to_strings(ordered_set))
            new_data.writelines(files_merger)

        # Delete temp file and rename it
        os.replace(self._temp_data_file_path, self._data_file_path)

        # Clear RAM set
        self._internal_set.clear()

    # Exposed functions
    def add(self, element):
        self._internal_set.add(self._apply_encode_function(element))
        # Check if set is full
        if len(self._internal_set) == MAX_ELEMENTS_IN_RAM:
            self._flush_set()


    def elements(self):
        self._flush_set()

        with open(self._data_file_path, "r") as data_file:
            for line in data_file:
                yield self._apply_decode_function(line)
