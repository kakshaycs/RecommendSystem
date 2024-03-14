import random
from faker import Faker

class TestDataGenerator:
    def __init__(self, num_samples):
        self.fake = Faker()
        self.num_samples = num_samples

    def generate_test_data(self):
        test_data = []
        for _ in range(self.num_samples):
            song = self.fake.name()
            singer = self.fake.name()
            length = random.randint(120, 600)  # Random length in seconds (2 to 10 minutes)
            size = random.randint(1, 100)      # Random size in MB
            test_data.append({'song': song, 'singer': singer, 'length': length, 'size': size})
        return test_data

# Example usage:
num_samples = 100
data_generator = TestDataGenerator(num_samples)
test_data = data_generator.generate_test_data()

# Print the first few samples
for i in range(5):
    print(test_data[i])
