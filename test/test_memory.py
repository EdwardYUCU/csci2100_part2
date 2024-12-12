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
