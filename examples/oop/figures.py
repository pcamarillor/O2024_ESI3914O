import math

# Base class
class Figure:
    def __init__(self, name):
        self.name = name
    
    def area(self):
        raise NotImplementedError("Subclasses must implement this method")
    
    def perimeter(self):
        raise NotImplementedError("Subclasses must implement this method")
    
    def display(self):
        print(f"Figure: {self.name}")
        print(f"Area: {self.area()}")
        print(f"Perimeter: {self.perimeter()}")

# Inherited class for Circle
class Circle(Figure):
    def __init__(self, radius):
        super().__init__("Circle")
        self.radius = radius
    
    def area(self):
        return math.pi * self.radius ** 2
    
    def perimeter(self):
        return 2 * math.pi * self.radius

# Inherited class for Triangle
class Triangle(Figure):
    def __init__(self, side_a, side_b, side_c):
        super().__init__("Triangle")
        self.side_a = side_a
        self.side_b = side_b
        self.side_c = side_c
    
    def area(self):
        # Using Heron's formula to calculate the area
        s = (self.side_a + self.side_b + self.side_c) / 2
        return math.sqrt(s * (s - self.side_a) * (s - self.side_b) * (s - self.side_c))
    
    def perimeter(self):
        return self.side_a + self.side_b + self.side_c

# Inherited class for Rectangle
class Rectangle(Figure):
    def __init__(self, width, height):
        super().__init__("Rectangle")
        self.width = width
        self.height = height
    
    def area(self):
        return self.width * self.height
    
    def perimeter(self):
        return 2 * (self.width + self.height)

# Example usage
if __name__ == "__main__":
    # Create instances of each figure
    circle = Circle(radius=5)
    triangle = Triangle(side_a=3, side_b=4, side_c=5)
    rectangle = Rectangle(width=4, height=6)
    
    # Display their details
    circle.display()
    print()
    triangle.display()
    print()
    rectangle.display()
