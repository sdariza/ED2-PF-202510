def generate_3smooth_gaps(n):
    gaps = set()
    p = 1
    while p < n:
        q = p
        while q < n:
            gaps.add(q)
            q *= 3
        p *= 2
    return sorted(gaps, reverse=True)[:5]

def shellsort(arr):
    n = len(arr)
    gaps = generate_3smooth_gaps(n)
    for gap in gaps:
        for i in range(gap, n):
            temp = arr[i]
            j = i
            while j >= gap and arr[j - gap] > temp:
                arr[j] = arr[j - gap]
                j -= gap
            arr[j] = temp
    return arr

def shellsort_wrapper(arr):
    return shellsort(arr[:]) 