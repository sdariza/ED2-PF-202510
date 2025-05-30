import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from algorithms.shellsort import shellsort_wrapper

def test_shellsort_basic():
    assert shellsort_wrapper([9, 7, 5, 3]) == [3, 5, 7, 9]

def test_shellsort_single_element():
    assert shellsort_wrapper([1]) == [1]