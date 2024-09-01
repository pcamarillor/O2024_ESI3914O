def solution(numbers) -> int:
    n = len(numbers)
    for i in range(n+1):
        if i not in numbers:
            return i
