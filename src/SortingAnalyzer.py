import time

class SortingAnalyzer:
    def __init__(self, dataset):
        self.dataset = dataset

    def quicksort(self, low=0, high=None):
        if high is None:
            high = len(self.dataset) - 1
        if low < high:
            pi = self.partition(low, high)
            self.quicksort(low, pi - 1)
            self.quicksort(pi + 1, high)

    def partition(self, low, high):
        pivot = self.dataset[high]
        i = low - 1
        for j in range(low, high):
            if self.dataset[j] <= pivot:
                i += 1
                self.dataset[i], self.dataset[j] = self.dataset[j], self.dataset[i]
        self.dataset[i + 1], self.dataset[high] = self.dataset[high], self.dataset[i + 1]
        return i + 1

    def mergesort(self, dataset=None):
        if dataset is None:
            dataset = self.dataset
        if len(dataset) > 1:
            mid = len(dataset) // 2
            L = dataset[:mid]
            R = dataset[mid:]

            self.mergesort(L)
            self.mergesort(R)

            i = j = k = 0
            while i < len(L) and j < len(R):
                if L[i] < R[j]:
                    dataset[k] = L[i]
                    i += 1
                else:
                    dataset[k] = R[j]
                    j += 1
                k += 1

            while i < len(L):
                dataset[k] = L[i]
                i += 1
                k += 1

            while j < len(R):
                dataset[k] = R[j]
                j += 1
                k += 1

    def analyze_sorting(self, sort_func):
        start_time = time.time()
        sort_func()
        end_time = time.time()
        return end_time - start_time

# Example usage
dataset = [64, 34, 25, 12, 22, 11, 90]
analyzer = SortingAnalyzer(dataset)

# Analyze quicksort
quicksort_time = analyzer.analyze_sorting(analyzer.quicksort)
print(f"Quicksort took {quicksort_time} seconds")

# Analyze mergesort
mergesort_time = analyzer.analyze_sorting(analyzer.mergesort)
print(f"Mergesort took {mergesort_time} seconds")
