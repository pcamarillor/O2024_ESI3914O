from collections import Counter

def solution(nums) -> int:
    n = len(nums)
    count = Counter(nums)
    print(count)

    for i in range(n + 1):
        if count[i] == 0:
            return i
