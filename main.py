import memory
import argparse


def external_merge_sort(B, b, N, T, input_file, output_file):
    buffer_pool = memory.BufferPool(B, b)
    buffer_pool_manager = memory.BufferPoolManager(buffer_pool)
    sec_store = memory.SecStore()
    sec_man = memory.SecStoreManager(sec_store, T)

    sec_store.read_file(input_file)

    nums = buffer_pool_manager.allocate(B // b // 2)
    buf = buffer_pool_manager.allocate(b // b // 2)
    for k in range(0, N, B // 2):
        sec_man.read("input", k, B // 2, nums)
        merge_sort(nums, buf, k, k + len(nums) - 1)
        sec_man.write(f"sort{k}", k, B // 2, nums)

    num_run = N // B * 2
    num_block = B // b

    block_per_run = num_block // (num_run + 1)
    block_buf = num_block - num_run * block_per_run
    runs = []
    for k in range(0, num_run):
        runs.append(buffer_pool_manager.allocate(block_per_run))
        sec_man.read(f"sort{k}", 0, block_per_run * b, runs[k])
    
    buf = buffer_pool_manager.allocate(block_buf)


def merge(nums, buf, left: int, mid: int, right: int):
    """merge the two sub array"""
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
    """Merge sort the array"""

    if left >= right:
        return
    mid = (left + right) // 2
    merge_sort(nums, buf, left, mid)
    merge_sort(nums, buf, mid + 1, right)
    merge(nums, buf, left, mid, right)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Path to input file")
    parser.add_argument("output", help="Path to output file")
    args = parser.parse_args()

    # Parameters
    B = 10000  # Buffer pool size in words
    b = 250  # Block size in words
    N = 200000  # Number of records
    T = 64  # Relative time taken for secStore access

    external_merge_sort(B, b, N, T, args.input, args.output)


if __name__ == "__main__":
    main()
