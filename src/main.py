import json

def load_config(file_path):
    with open(file_path) as f:
        data = json.load(f)
    return data

def calculate(a, b):
    if(b == 0):
        print("Error: Division by zero, defaulting result by 0")
        return 0
    return a / b

def main():
    config = load_config("config.json")
    result = calculate(config["a"], config["b"])
    print("Result:", result)

if __name__ == "__main__":
    main()
