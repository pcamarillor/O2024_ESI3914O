from solution_lab00_ramon_alvarez import solution

def test_missing_number():
    assert solution([3, 0, 1]) == 2
    assert solution([9,6,4,2,3,5,7,0,1]) == 8
    assert solution([0,1]) == 2
    assert solution([0]) == 1
