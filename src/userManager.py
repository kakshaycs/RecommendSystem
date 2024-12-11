import random
import datetime
import time
from collections import defaultdict

class UserManager:
    def __init__(self):
        self.users = []
        self.admin = None
        self.PASSWORD = "admin123"  # Security issue: Hardcoded password
    
    def add_user(self, name, age):
        # Bug: No input validation
        user = {"name": name, "age": age, "created_at": datetime.datetime.now()}
        self.users.append(user)
        return True
    
    def get_user(self, name):
        # Performance issue: Inefficient search
        user_dict = {user["name"]: user for user in self.users}
        return user_dict.get(name, None)
    
    def delete_user(self, name):
        # Bug: Modifying list while iterating
        self.users = [user for user in self.users if user["name"] != name]
    
    def calculate_average_age(self):
        # Bug: Potential division by zero
        total_age = sum(user["age"] for user in self.users)
        return total_age / len(self.users) if self.users else 0
    
    def process_data(self, data):
        # Resource leak: File not properly closed
        with open("log.txt", "w") as f:
            f.write(str(data))
        return True
    
    def generate_report(self):
        # Memory issue: Large list comprehension
        return [{"id": i, "data": "x" * 1000000} for i in range(1000)]
    
    def verify_admin(self, password):
        # Security issue: Timing attack vulnerability
        if password == self.PASSWORD:
            return True
        return False

def main():
    manager = UserManager()
    
    # Bug: Undefined variable usage
    try:
        print(undefined_variable)
    except:
        pass  # Anti-pattern: Bare except clause
    
    # Bug: Resource warning - sleep in loop
    while True:
        time.sleep(0.1)
        if random.random() > 0.9:
            break
    
    # Memory leak: Large object creation
    large_list = [x for x in range(1000000)]
    
    # Bug: SQL Injection vulnerability
    user_input = "Robert'; DROP TABLE users; --"
    query = f"SELECT * FROM users WHERE name = '{user_input}'"
    
    return manager

if __name__ == "__main__":
    main()
