import json

def load_config(file_path):
    try:
        with open(file_path) as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        data = {"a": 10, "b": 2}
    return data

def calculate(a, b):
    try:
        return a / b
    except ZeroDivisionError:
        print("Error: Division by zero, sending response 0")
        return 0

def main():
    config = load_config("config.json")
    result = calculate(config.get("a", 1), config.get("b", 1))
    print("Result:", result)

if __name__ == "__main__":
    main()
