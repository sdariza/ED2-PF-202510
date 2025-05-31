import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from algorithms.quicksort import quicksort_wrapper

def test_quicksort_basic():
    assert quicksort_wrapper([3, 1, 2]) == [1, 2, 3]

def test_quicksort_empty():
    assert quicksort_wrapper([]) == []

def test_quicksort_sorted():
    assert quicksort_wrapper([1, 2, 3]) == [1, 2, 3]