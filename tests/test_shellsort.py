from shellsort import shellsort

def test_shellsort_basic():
    assert shellsort([9, 7, 5, 3]) == [3, 5, 7, 9]

def test_shellsort_single_element():
    assert shellsort([1]) == [1]