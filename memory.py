import collections.abc as abc
import operator
import math


class BufferPool:
    """A limited main memory."""

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


class memory_block:
    """A block of memory in the BufferPool."""

    def __init__(self, memory: BufferPool, size: int, start_address: int, b: int = 1):
        self._memory = memory
        self._size = size
        self._start_address = start_address
        self._b = b
        self.offset = 0
        self.modified = 0

    @property
    def size(self):
        if self.modified:
            return self.modified
        return self._size * self._b

    @size.setter
    def size(self, value):
        self.modified = value

    @property
    def start_address(self):
        return self._start_address * self._b + self.offset

    def __len__(self):
        return self.size - self.offset

    def __getitem__(self, key):
        if isinstance(key, slice):
            key = key.indices(self.size)
            key = slice(
                key[0] + self.start_address, key[1] + self.start_address, key[2]
            )
            return self._memory[key]
        if key >= self.size or key < 0:
            raise IndexError("Index out of bound")
        return self._memory[self.start_address + operator.index(key)]

    def __setitem__(self, key, obj):
        if isinstance(key, slice):
            key = key.indices(self.size)
            key = slice(
                key[0] + self.start_address, key[1] + self.start_address, key[2]
            )
            self._memory[key] = obj
            return
        if key >= self.size or key < 0:
            raise IndexError("Index out of bound")
        self._memory[self.start_address + operator.index(key)] = obj


class File(abc.MutableSequence):
    """The abstraction of the data in SecStore."""

    def __init__(self, name: str, data=(), /):
        """Initialize a File object with a name and data."""
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

    def __eq__(self, other):
        return self._data == other

    @property
    def name(self) -> str:
        return self._name

    def read(self, start: int, size: int):
        """Read a portion of the file."""
        self[start]
        return self[start : start + size].copy()

    def write(self, start: int, size: int, data: memory_block):
        """Write data to the file."""
        self[start : start + size] = data[:size]


class SecStore(abc.MutableMapping):
    """The unlimited store space."""

    def __init__(self):
        self._disk: dict[str, File] = {
            "input": File("input", []),
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
        Read a file into SecStore.
        SecStore is used for the input file inputs.txt, a text file containing
        floating point numbers to be sorted.
        """
        with open(file_name) as fp:
            for line in fp:
                self["input"].append(float(line))

    def write_file(self, file_name: str):
        """
        Write the SecStore data to a file.
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
        """Write data to disk."""
        if file_name not in self:
            self[file_name] = File(file_name, data[:size])
        else:
            self[file_name].write(start, size, data)


class SecStoreManager:
    """Manage the SecStore for read and write operations.
    Also for recording the total overhead."""

    def __init__(self, store: SecStore, T: int, b: int, /):
        self.store = store
        self.T = T
        self.b = b
        self.H = 0

    def read(
        self, file_name: str, start: int, size: int, buffer_blocks: memory_block
    ) -> int:
        """Read the file in SecStore into BufferPool.
        buffer_blocks is the write handle that needs to be
        provided by the caller."""
        data = self.store.read(file_name, start, size)
        self.H += math.ceil(len(data) / self.b) * self.T
        buffer_blocks[: len(data)] = data
        return len(data)

    def write(self, file_name: str, start: int, size: int, buffer_blocks: memory_block):
        """Write data from BufferPool to SecStore."""
        self.store.write(file_name, start, size, buffer_blocks)
        self.H += math.ceil(size / self.b) * self.T


class BufferPoolManager:
    """Manage the buffer_pool."""

    def __init__(self, buffer_pool: BufferPool):
        self.buffer_pool = buffer_pool

    def allocate(self, num_blocks: int) -> memory_block | None:
        """Allocate a number of blocks in the buffer pool."""
        count = 0
        for i, status in enumerate(self.buffer_pool.status):
            if status:
                count += 1
            else:
                count = 0
            if count == num_blocks:
                self.buffer_pool.status[i - num_blocks + 1 : i + 1] = [
                    False
                ] * num_blocks
                return memory_block(
                    self.buffer_pool, num_blocks, i - num_blocks + 1, self.buffer_pool.b
                )
        return None

    def free(self, buffer_blocks: memory_block):
        """Free the allocated blocks in the buffer pool."""
        self.buffer_pool.status[
            buffer_blocks._start_address : buffer_blocks._start_address
            + buffer_blocks._size
        ] = [True] * buffer_blocks._size
