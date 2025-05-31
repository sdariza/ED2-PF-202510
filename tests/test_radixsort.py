from algorithms.radixsort import radixsort_wrapper


def test_radixsort_basic():
    assert radixsort_wrapper([170, 45, 75, 90, 802, 24, 2, 66]) == [2, 24, 45, 66, 75, 90, 170, 802]

def test_radixsort_empty():
    assert radixsort_wrapper([]) == []

def test_radixsort_single_element():
    assert radixsort_wrapper([5]) == [5]

def test_radixsort_duplicates():
    assert radixsort_wrapper([4, 2, 4, 1, 3]) == [1, 2, 3, 4, 4]

def test_radixsort_all_equal():
    assert radixsort_wrapper([7, 7, 7]) == [7, 7, 7]