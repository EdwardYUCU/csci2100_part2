from memory import (
    File,
    SecStore,
    BufferPool,
    BufferPoolManager,
    memory_block,
    SecStoreManager,
)
from main import merge_sort
import random
import pytest


def test_file():
    data = list(random.random() * 100 for _ in range(10))
    f = File("f", data)
    a = f.read(2, 5)
    assert a == data[2:7]

    modi = list(random.random() * 100 for _ in range(3))
    f.write(3, 3, memory_block(modi, 3, 0))
    assert f[3:6] == modi


def test_secstore():
    input_file = "inputs/inputs.txt"
    output_file = "outputs/sorted.txt"
    sec = SecStore()
    sec.read_file(input_file)
    sec["output_sort"] = sorted(sec["input"])
    output = []
    with open(output_file) as fp:
        for line in fp:
            output.append(float(line))

    assert output == sec["output_sort"]


def test_allocation():
    buffer_pool = BufferPool(10000, 100)
    buffer_man = BufferPoolManager(buffer_pool)
    l = buffer_man.allocate(20)
    min(l[:10])
    assert l.start_address == 0
    assert l[:] == [0] * 2000
    assert not any(buffer_pool.status[:20])
    assert buffer_pool.status[20]

    l1 = buffer_man.allocate(1)
    assert not buffer_pool.status[20]

    l2 = buffer_man.allocate(5)
    assert not any(buffer_pool.status[21:26])

    buffer_man.free(l1)
    assert buffer_pool.status[20]

    buffer_man.free(l)

    newl = buffer_man.allocate(5)
    assert not any(buffer_pool.status[:5])

    buffer_man.free(newl)
    buffer_man.free(l2)
    assert all(buffer_pool.status)


def test_read():
    input_file = "inputs/inputs.txt"
    output_file = "outputs/sorted.txt"
    sec = SecStore()
    sec_man = SecStoreManager(sec, 2, 100)
    sec.read_file(input_file)

    buffer_pool = BufferPool(10000, 100)
    buffer_man = BufferPoolManager(buffer_pool)

    buf = buffer_man.allocate(50)

    sec_man.read("input", 0, 50 * 100, buf)
    assert buf[0] == sec["input"][0]
    assert buf[:] == sec["input"][:5000]
    assert sorted(buf) == sorted(sec["input"][:5000])

    with pytest.raises(Exception):
        sec_man.read("input", 200000, 100, buf)


def test_read_write_partial():
    B, b, T, N = 10000, 100, 2, 200000
    input_file = "inputs/inputs.txt"

    buffer_pool = BufferPool(B, b)
    buffer_pool_manager = BufferPoolManager(buffer_pool)
    sec_store = SecStore()
    sec_man = SecStoreManager(sec_store, T, b)

    sec_store.read_file(input_file)
    nums = buffer_pool_manager.allocate(5)
    sec_man.read("input", 0, 200, nums)
    assert nums[200] == 0
    assert all(k != 0 for k in nums[:200])

    sec_man.write("output", 0, 100, nums)
    assert sec_store["output"][:100] == nums[:100]
    with pytest.raises(IndexError):
        sec_store["output"][100]


def test_sort():
    B, b, T, N = 10000, 100, 2, 200000
    input_file = "inputs/inputs.txt"

    buffer_pool = BufferPool(B, b)
    buffer_pool_manager = BufferPoolManager(buffer_pool)
    sec_store = SecStore()
    sec_man = SecStoreManager(sec_store, T, b)

    sec_store.read_file(input_file)

    nums = buffer_pool_manager.allocate(B // b // 2)
    assert nums.start_address == 0
    buf = buffer_pool_manager.allocate(B // b // 2)
    assert buf._start_address == B // b // 2

    for k in range(0, N, B // 2):
        sec_man.read("input", k, B // 2, nums)
        merge_sort(nums, buf, 0, len(nums) - 1)
        sec_man.write(f"sort{k}", k, B // 2, nums)
    assert all(
        sec_store[f"sort{k}"] == sorted(sec_store[f"sort{k}"])
        for k in range(0, N, B // 2)
    )
    buffer_pool_manager.free(nums)
    buffer_pool_manager.free(buf)
