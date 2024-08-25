# Given an array nums containing n distinct numbers within the range
# [0, n], return the single number in the range that is missing from the
# array.

def solution(nums) -> int:
    n = len(nums)

    expected = 0

    for i in range(n+1):
        expected += i 

    actual = sum(nums)
    
    return expected - actual
