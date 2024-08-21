"""
Given an array nums containing n distinct numbers within the range
[0, n], return the single number in the range that is missing from the
array.
"""

def solution(nums:list[int]) -> int:
    length = len(nums)
    nums.sort()
    #print('LENGTH',length)
    #print(nums)
    for i in range(0, length):
        curr = nums[i]
        #print(i, curr)
        if curr != i:
            #print('Im returning: ', i)
            return i
        
    return length