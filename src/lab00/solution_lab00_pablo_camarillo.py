def solution(nums) -> int:
    n = len(nums)
    for i in range(n + 1):
        if i not in nums:
            return i
    return -1