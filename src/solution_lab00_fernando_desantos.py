
def solution(nums) -> int:
    # Gauss sum formula n(n+1) // 2
    gauss_sum = len(nums) * (len(nums) + 1) // 2  
    return gauss_sum - sum(nums)  

