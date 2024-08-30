from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "FilterExample")

# Step 1: Create an RDD with a list of integers
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
numbers_rdd = sc.parallelize(numbers)

# Step 2: Use filter to keep only even numbers
def filter_function(x):
    return x % 2 == 0

even_numbers_rdd = numbers_rdd.filter(lambda x: x % 2 == 0)
even_numbers_rdd_function = numbers_rdd.filter(filter_function)


# Step 3: Collect and print the results
even_numbers = even_numbers_rdd.collect()
print(f"Even numbers using lambda expression:{even_numbers}")

# Step 4: Collect and print the results obtained by using a function as argument
even_numbers_rdd_function = even_numbers_rdd_function.collect()
print(f"Even numbers using functions as argument:{even_numbers_rdd_function}")

# Stop the SparkContext
sc.stop()
