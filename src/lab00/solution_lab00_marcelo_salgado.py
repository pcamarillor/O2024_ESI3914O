def solution(nums) ->  int:
    length = len(nums)
    nums = sorted(nums)
    for i in range(length + 1):
        if i == length:
            return length;
        elif i != nums[i]:
            return i
        
        
print(solution([0,1,2,3,4]))