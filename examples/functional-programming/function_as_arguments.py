def apply_function(func, value):
    return func(value)
  
def square(x):
    return x * x
  
# Passing the function `square` as an argument to 
# `apply_function`
result = apply_function(square, 5)
print(result)