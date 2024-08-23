def solution(nums) -> int:
    n = len(nums)
    actual_sum = 0
    nums_sum = 0
    
    for i in range(n + 1):
        actual_sum += i
    
    for i in range(n):
        nums_sum += nums[i]
    
    return actual_sum - nums_sum
