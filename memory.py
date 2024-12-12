import sys
from copy import copy
import collections.abc as abc
from dataclasses import dataclass
import operator


@dataclass(frozen=True)
class memory_block:
    _memory: list[float]
    _size: int
    _start_address: int
    _b: int = 1

    @property
    def size(self):
        return self._size * self._b

    @property
    def start_address(self):
        return self._start_address * self._b

    def __len__(self):
        return self.size

    def __getitem__(self, key):
        if isinstance(key, slice):
            key = key.indices(self.size)
            key = slice(
                key[0] + self.start_address, key[1] + self.start_address, key[2]
            )
            return self._memory[key]
        return self._memory[self.start_address + key]

    def __setitem__(self, position, obj):
        if isinstance(key, slice):
            key = key.indices(self.size)
            key = slice(
                key[0] + self.start_address, key[1] + self.start_address, key[2]
            )
            self._memory[key] = obj
        self._memory[self.start_address + key] = obj


class File(abc.MutableSequence):
    """The abstraction of the data in SecStore"""

    def __init__(self, name: str, data: abc.Iterable = (), /):
        """Initial a NAME object with DATA"""
        self._data = list(data)
        self._name = name

    def __len__(self):
        return len(self._data)

    def __getitem__(self, index):
        return self._data[index]

    def __setitem__(self, index, obj):
        self._data[index] = obj

    def __delitem__(self, index):
        del self._data[index]

    def insert(self, index, value):
        self._data.insert(index, value)

    @property
    def name(self) -> str:
        return self._name

    def read(self, start: int, size: int):
        return self[start : start + size].copy()

    def write(self, start: int, size: int, data: memory_block):
        self[start : start + size] = data[:]


class SecStore(abc.MutableMapping):
    """The unlimited store space"""

    def __init__(self):
        self._disk: dict[str, File] = {
            "input": File("input", []),
            "output": File("output", []),
        }

    def __getitem__(self, key):
        return self._disk[key]

    def __setitem__(self, key, value):
        self._disk[key] = value

    def __delitem__(self, key):
        del self._disk[key]

    def __len__(self):
        return len(self._disk)

    def __iter__(self):
        return iter(self._disk)

    def read_file(self, file_name: str):
        """
        SecStore is used for the input file inputs.txt, a text file containing
        floating point numbers to be sorted.
        """
        with open(file_name) as fp:
            for line in fp:
                self["input"].append(float(line))

    def write_file(self, file_name: str):
        """
        The store is also used for the output file sorted.txt that you output
        (in CSV format).
        """
        with open(file_name, "w") as fp:
            for num in self["output"]:
                print(num, file=fp)

    def read(self, file_name: str, start: int, size: int):
        """Read file from disk. Return a sequence."""
        return self[file_name].read(start, size)

    def write(self, file_name: str, start: int, size: int, data: memory_block):
        "Write data to disk"
        assert size == len(data)
        self[file_name].write(start, size, data)


class BufferPool:
    """A limited main memory"""

    def __init__(self, B, b, /):
        self.B = B
        self.b = b
        self._memory = [0] * B
        self.status = [True] * (B // b)

    def __len__(self):
        return self.B

    def __getitem__(self, index):
        return self._memory[index]

    def __setitem__(self, index, obj):
        self._memory[index] = obj


class SecStoreManager:
    """Manage the SecStore for read and write operations.
    Also for recording the total overhead"""

    def __init__(self, store: SecStore, T: int, b: int, /):
        self.store = store
        self.T = T
        self.b = b
        self.H = 0

    def read(self, file_name: str, start: int, size: int, buffer_blocks: memory_block):
        """Read the file in SecStore into BufferPool
        buffer_blocks is the write handle that need to be
        provided by the caller"""
        try:
            try:
                data = self.store.read(file_name, start, size)
            except IndexError as err:
                print(f"Start position is invalid or size is too large\n{err}")
            except KeyError as err:
                print(f"{file_name} not found in SecStore\n{err}")
            else:
                self.H += size // self.b * self.T
                buffer_blocks[:size] = data
        except ValueError as err:
            print(f"Size is not correct\n{err}")

    def write(self, file_name: str, start: int, size: int, buffer_blocks: memory_block):
        self.store.write(start, size, buffer_blocks)
        self.H += size // self.b * self.T


class BufferPoolManager:
    """Manage the buffer_pool"""

    def __init__(self, buffer_pool: BufferPool):
        self.buffer_pool = buffer_pool

    def allocate(self, num_blocks: int) -> memory_block:
        count = 0
        for i, status in enumerate(self.buffer_pool.status):
            if status:
                count += 1
            else:
                count = 0
            if count == num_blocks:
                return memory_block(self.buffer_pool, num_blocks, i - num_blocks + 1, self.buffer_pool.b)

    def free(self, buffer_blocks: memory_block):
        self.buffer_pool.status[
            buffer_blocks._start_address : buffer_blocks._start_address
            + buffer_blocks._size
        ] = [True] * buffer_blocks._size
