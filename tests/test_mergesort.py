from mergesort import mergesort

def test_mergesort_basic():
    assert mergesort([4, 2, 5, 1]) == [1, 2, 4, 5]

def test_mergesort_duplicates():
    assert mergesort([3, 3, 2, 1]) == [1, 2, 3, 3]