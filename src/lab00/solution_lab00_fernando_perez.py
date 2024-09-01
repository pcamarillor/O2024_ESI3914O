def solution(nums) -> int:
    for i in range(0,len(nums)+1):
        if i not in nums:
            return i
