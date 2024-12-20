# External merge sort

A project to implement external merge sort and a virtual machine with limited buffer.

## Structure

The project has a `README` file, a `LICENSE` under the root directory, together with the `inputs` and `outputs` sub-directory which will contain the output of the program.

The most important file is `main.py` and `memory.py`. The virtual machine implementation is in `memory.py` and the external sort function is in `main.py`.

## Build

The project is written on Linux and I don't test it on other platforms. You need at least python3.12 to run the program.

The program based solely on the standard library of python3.

To run the program, run `$python3 main.py input output`. Or you can run `$python3 main.py -h` to get help message.