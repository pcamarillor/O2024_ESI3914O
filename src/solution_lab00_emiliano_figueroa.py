def solution(nums):
    set_nums = set(nums)
    set_complete = set(range(len(nums) + 1))
    return (set_nums ^ set_complete).pop()
