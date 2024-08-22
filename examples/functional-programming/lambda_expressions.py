# Define some simple functions
def add(x, y):
    return x + y

def subtract(x, y):
    return x - y

def multiply(x, y):
    return x * y

def divide(x, y):
    if y != 0:
        return x / y
    else:
        return "Division by zero error"

# A function that takes another function as an argument and a list of tuples
def operate_on_list(operation, data):
    return [operation(x, y) for x, y in data]

# Example usage with lambda expressions
data = [(10, 2), (15, 5), (20, 4), (25, 0)]

# Add using lambda and the add function
result_add = operate_on_list(lambda x, y: add(x, y), data)
print(f"Addition Results: {result_add}")

# Subtract using lambda and the subtract function
result_subtract = operate_on_list(lambda x, y: subtract(x, y), data)
print(f"Subtraction Results: {result_subtract}")

# Multiply using lambda and the multiply function
result_multiply = operate_on_list(lambda x, y: multiply(x, y), data)
print(f"Multiplication Results: {result_multiply}")

# Divide using lambda and the divide function
result_divide = operate_on_list(lambda x, y: divide(x, y), data)
print(f"Division Results: {result_divide}")
