class Calculator:
    """A class for basic arithmetic operations."""

    def __init__(self, num1, num2):
        """Initialize Calculator with two numbers."""
        self.num1 = num1
        self.num2 = num2

    def add(self):
        """Return the sum of two numbers."""
        return self.num1 + self.num2

    def subtract(self):
        """Return the difference between two numbers."""
        return self.num1 - self.num2

    def multiply(self):
        """Return the product of two numbers."""
        return self.num1 * self.num2

    def divide(self):
        """Return the division of two numbers."""
        if self.num2 != 0:
            return self.num1 / self.num2
        else:
            return "Cannot divide by zero!"

    def power(self):
        """Return the power of num1 to num2."""
        return self.num1 ** self.num2

    def modulus(self):
        """Return the modulus of two numbers."""
        return self.num1 % self.num2

    def floor_divide(self):
        """Return the floor division of two numbers."""
        return self.num1 // self.num2

    def square_root(self):
        """Return the square root of a number."""
        return self.num1 ** 0.5

    def factorial(self):
        """Return the factorial of a number."""
        result = 1
        for i in range(1, self.num1 + 1):
            result *= i
        return result

    def prime(self):
        """Check if a number is prime."""
        if self.num1 <= 1:
            return False
        for i in range(2, int(self.num1 ** 0.5) + 1):
            if self.num1 % i == 0:
                return False
        return True

    def is_even(self):
        """Check if a number is even."""
        return self.num1 % 2 == 0

    def is_odd(self):
        """Check if a number is odd."""
        return self.num1 % 2 != 0

    def binary_representation(self):
        """Return the binary representation of a number."""
        return bin(self.num1)

    def hexadecimal_representation(self):
        """Return the hexadecimal representation of a number."""
        return hex(self.num1)

    def octal_representation(self):
        """Return the octal representation of a number."""
        return oct(self.num1)

    def to_string(self):
        """Convert a number to string."""
        return str(self.num1)

    def calculate_area(shape, **kwargs):
        """
        Calculate the area of a shape.

        Parameters:
        shape (str): The type of shape ('rectangle', 'circle', 'triangle')
        **kwargs: Additional parameters required for the shape calculation

        Returns:
        float: The calculated area of the shape
        """
        if shape == 'rectangle':
            length = kwargs.get('length')
            width = kwargs.get('width')
            return length * width
        elif shape == 'circle':
            radius = kwargs.get('radius')
            return 3.14 * radius ** 2
        elif shape == 'triangle':
            base = kwargs.get('base')
            height = kwargs.get('height')
            return 0.5 * base * height
        else:
            raise ValueError("Invalid shape type. Please use 'rectangle', 'circle', or 'triangle'.")

    def main():
        """Main function to demonstrate Calculator class."""
        num1 = int(input("Enter first number: "))
        num2 = int(input("Enter second number: "))
        calc = Calculator(num1, num2)
        print("Sum:", calc.add())
        print("Difference:", calc.subtract())
        print("Product:", calc.multiply())
        print("Division:", calc.divide())
        print("Power:", calc.power())
        print("Modulus:", calc.modulus())
        print("Floor Division:", calc.floor_divide())
        print("Square Root of num1:", calc.square_root())
        print("Factorial of num1:", calc.factorial())
        print("Is num1 Prime:", calc.prime())
        print("Is num1 Even:", calc.is_even())
        print("Is num1 Odd:", calc.is_odd())
        print("Binary Representation of num1:", calc.binary_representation())
        print("Hexadecimal Representation of num1:", calc.hexadecimal_representation())
        print("Octal Representation of num1:", calc.octal_representation())
        print("String representation of num1:", calc.to_string())

if __name__ == "__main__":
    Calculator.main()
