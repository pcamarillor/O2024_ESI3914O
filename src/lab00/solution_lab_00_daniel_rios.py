def solution(nums):
    items = len(nums)
    expected_sum = items * (items + 1) // 2
    actual_sum = sum(nums)

    return expected_sum - actual_sum
