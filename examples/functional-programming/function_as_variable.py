def greet(name):
    return f"Hello, {name}!"
  
# Assigning the function to a variable
say_hello = greet
  
# Using the new variable to call the function
print(say_hello("Tom"))  # Output: Hello, Tom!