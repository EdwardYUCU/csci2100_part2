from memory import *
import random


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
