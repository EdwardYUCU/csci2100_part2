import memory
import argparse


def external_merge_sort(B, b, N, T, input_file, output_file):
    """Perform external merge sort on the input file and write the sorted data to the output file."""
    buffer_pool = memory.BufferPool(B, b)
    buffer_pool_manager = memory.BufferPoolManager(buffer_pool)
    sec_store = memory.SecStore()
    sec_man = memory.SecStoreManager(sec_store, T, b)

    # Read input file into SecStore
    sec_store.read_file(input_file)

    # Allocate memory blocks for sorting
    nums = buffer_pool_manager.allocate(B // b // 2)
    buf = buffer_pool_manager.allocate(B // b // 2)

    # Perform initial sorting of chunks
    for i, k in enumerate(range(0, N, B // 2)):
        sec_man.read("input", k, B // 2, nums)
        merge_sort(nums, buf, 0, len(nums) - 1)
        sec_man.write(f"sort{i}", k, B // 2, nums)
    buffer_pool_manager.free(nums)
    buffer_pool_manager.free(buf)

    num_run = N // (B // 2)
    num_block = B // b

    block_per_run = num_block // (num_run + 1)
    block_buf = num_block - num_run * block_per_run - 1
    runs = []
    for k in range(0, num_run):
        runs.append(buffer_pool_manager.allocate(block_per_run))
        sec_man.read(f"sort{k}", 0, block_per_run * b, runs[k])

    buf = buffer_pool_manager.allocate(block_buf)
    compare = buffer_pool_manager.allocate(1)
    j = 0
    for k in range(0, num_run):
        compare[k] = runs[k][0]
    output_pos = 0
    runs_count = [block_per_run * b] * num_run

    # Merge sorted runs
    while True:
        smallest = min(compare[:num_run])
        if smallest == float("inf"):
            sec_man.write("output", output_pos, j, buf)
            break

        small_pos = compare[:num_run].index(smallest)

        buf[j] = smallest
        j += 1
        runs[small_pos].offset += 1

        for k in range(0, num_run):
            if len(runs[k]) == 0:
                runs[k].offset = 0
                try:
                    length = sec_man.read(
                        f"sort{k}", runs_count[k], block_per_run * b, runs[k]
                    )
                except IndexError:
                    runs[k][0] = float("inf")
                    compare[k] = float("inf")
                    continue
                finally:
                    runs[k].size = length

                runs_count[k] += block_per_run * b
            compare[k] = runs[k][0]

        if j == block_buf * b:
            sec_man.write("output", output_pos, j, buf)
            output_pos += j
            j = 0
    sec_store.write_file(output_file)
    print(sec_man.H)


def merge(nums, buf, left: int, mid: int, right: int):
    """Merge the two subarrays."""
    i, j, k = left, mid + 1, left

    while i <= mid and j <= right:
        if nums[i] <= nums[j]:
            buf[k] = nums[i]
            i += 1
        else:
            buf[k] = nums[j]
            j += 1
        k += 1

    while i <= mid:
        buf[k] = nums[i]
        i += 1
        k += 1
    while j <= right:
        buf[k] = nums[j]
        j += 1
        k += 1

    for k in range(left, right + 1):
        nums[k] = buf[k]


def merge_sort(nums, buf, left: int, right: int):
    """Merge sort the array."""
    if left >= right:
        return
    mid = (left + right) // 2
    merge_sort(nums, buf, left, mid)
    merge_sort(nums, buf, mid + 1, right)
    merge(nums, buf, left, mid, right)


def main():
    """Main function to parse arguments and call external_merge_sort."""
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Path to input file")
    parser.add_argument("output", help="Path to output file")
    args = parser.parse_args()

    # Parameters
    Bs = [80000, 40000, 20000, 10000]  # Buffer pool size in words
    bs = [100, 200]  # Block size in words
    N = 200000  # Number of records
    Ts = [1, 4, 16, 128, 512]  # Relative time taken for secStore access

    for b in bs:
        for B in Bs:
            for T in Ts:
                print(f"B={B}, b={b}, T={T}")
                external_merge_sort(B, b, N, T, args.input, args.output)


if __name__ == "__main__":
    main()
