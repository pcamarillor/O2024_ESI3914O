def solution(nums) -> int:
    numsSorted = sorted(nums)
    n = len(nums)
    for i in range(n + 1):
        if i == n:
            return n
        if i != numsSorted[i]:
            return i

print(solution([1, 0, 2, 6, 4, 3]))