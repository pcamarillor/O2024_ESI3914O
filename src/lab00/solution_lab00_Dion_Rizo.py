def solution(nums) -> int:
    n = len(nums)
    expected_sum = n * (n + 1) // 2
    actual_sum = sum(nums)
    return expected_sum - actual_sum

# Testing the solution


if __name__ == "__main__":
    test_nums = [3, 0, 1]
    print(solution(test_nums))  
