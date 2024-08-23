def solution(nums) -> int:
    n = len(nums)
    expected_sum = n * (n + 1) // 2
    actual_sum = sum(nums)
    return expected_sum - actual_sum

array = [3, 0, 1, 4, 2]
print(solution(array))
