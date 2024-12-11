import memory
import argparse


def external_merge_sort(B, b, N, T, input_file, output_file):
    buffer_pool = memory.BufferPool(B, b)
    buffer_pool_manager = memory.BufferPoolManager(buffer_pool)
    sec_store = memory.SecStore()
    sec_man = memory.SecStoreManager(sec_store, T)

    sec_store.read_file(input_file)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("INPUT", help="Path to input file", dest="input")
    parser.add_argument("OUTPUT", help="Path to output file", dest="output")
    args = parser.parse_args()

    # Parameters
    B = 10000  # Buffer pool size in words
    b = 250  # Block size in words
    N = 200000  # Number of records
    T = 64  # Relative time taken for secStore access

    external_merge_sort(B, b, N, T, args.input, args.output)


if __name__ == "__main__":
    main()
