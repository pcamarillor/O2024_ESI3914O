#O(n)

def solution(num) -> int:
    n = len(num)  
    total = n * (n + 1) // 2  
    return total - sum(num)  

#prueba
num = [0,2]
print(solution(num))
