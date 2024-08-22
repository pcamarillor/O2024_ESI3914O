def add(x, y):
    return x + y
  
def subtract(x, y):
    return x - y
  
# Store functions in a dictionary
operations = {
    'add': add,
    'subtract': subtract
}
  
# Use the dictionary to call functions
print(operations['add'](10, 5))
print(operations['subtract'](10, 5))