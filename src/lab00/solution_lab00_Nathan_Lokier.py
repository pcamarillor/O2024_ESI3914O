def solution(nums):
    n = len(nums)
    expected_sum = n * (n + 1) // 2
    numbers_sum = sum(nums)
    return expected_sum - numbers_sum
