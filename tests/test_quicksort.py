from quicksort import quicksort

def test_quicksort_basic():
    assert quicksort([3, 1, 2]) == [1, 2, 3]

def test_quicksort_empty():
    assert quicksort([]) == []

def test_quicksort_sorted():
    assert quicksort([1, 2, 3]) == [1, 2, 3]