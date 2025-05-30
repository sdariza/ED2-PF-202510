from radixsort import radixsort

def test_radixsort_basic():
    assert radixsort([170, 45, 75, 90, 802, 24, 2, 66]) == [2, 24, 45, 66, 75, 90, 170, 802]

def test_radixsort_with_zeros():
    assert radixsort([0, 5, 0, 2]) == [0, 0, 2, 5]