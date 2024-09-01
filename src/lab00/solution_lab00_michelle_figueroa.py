from collections import Counter

def solution(nums):
    n = len(nums)
    count = Counter(range(n + 1))
    count.subtract(nums)
    for num in count:
        if count[num] == 1:
            return num

nums = [3, 0, 1]
print(solution(nums))