from array import array
from memory import *
import random

def test_file():
    data = list(random.random()* 100 for _ in range(10))
    f = File("f", data)
    a = f.read(2, 5)
    assert a == array('d', data[2:7])

    modi = list(random.random()  *100 for _ in range(3))
    f.write(3, 6, memory_ptr(memoryview(array('d',modi)), 0))
    assert f[3:6] == array('d', modi)

def test_secstore():
    input_file = "input/inputs.txt"
    output_file = "output/sorted.txt"
    sec = SecStore()
    sec.read_file(input_file)
    sec["output_sort"] = sorted(sec["input"])
    output = []
    with open(output_file) as fp:
        for line in fp:
            output.append(float(line))

    assert output == sec["output_sort"]


