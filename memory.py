from array import array
import sys
from copy import copy
import collections.abc as abc


class File(abc.MutableSequence):
    """The abstraction of the data in SecStore"""

    def __init__(self, name: str, data: abc.Iterable):
        """Initial a NAME object with DATA"""
        self._data = array("d", list(data))
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
        return copy(self[start : start + size])

    def write(self):
        pass


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


class BufferPool(abc.MutableSequence):
    """A limited main memory"""

    def __init__(self, B, b):
        self.B = B
        self.b = b
        self._memory = array("d", [0] * B)
        self.status = [True] * (B // b)

    def __len__(self):
        return self.B

    def __getitem__(self, index):
        return self._memory[index]

    def __setitem__(self, index, obj):
        self._memory[index] = obj

    def __delitem__(self, index):
        del self._memory[index]

    def insert(self, index, obj):
        self._memory.insert(index, obj)


class SecStoreManager:
    """Manage the SecStore for read and write operations.
    Also for recording the total overhead"""

    def __init__(self, store: SecStore, T: int):
        self.store = store
        self.T = T
        self.H = 0

    def read(
        self,
        file_name: str,
        start: int,
        size: int,
        buffer_blocks: memoryview
    ):
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

            buffer_blocks = data
        except ValueError as err:
            print(f"Size is not correct\n{err}")

    def write(self):
        pass


class BufferPoolManager:
    """Manage the buffer_pool"""

    def __init__(self, buffer_pool: BufferPool):
        self.buffer_pool = buffer_pool

    def allocate(self, num_blocks: int) -> memoryview | None:
        
