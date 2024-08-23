def solution(nums) -> int:
    # Calcula el tamaño esperado del arreglo
    n = len(nums)
    
    # Calcula la suma esperada de los números de 0 a n
    expected_sum = n * (n + 1) // 2
    
    # Calcula la suma actual de los números en el arreglo nums
    actual_sum = sum(nums)
    
    # El número faltante es la diferencia entre la suma esperada y la suma actual
    return expected_sum - actual_sum
