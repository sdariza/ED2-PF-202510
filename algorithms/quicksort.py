
def quicksort(data, low, high):
    if low < high:
        i = low
        j = high
        p = data[(low + high) // 2]
        
        while i <= j:
            while data[i] < p:
                i += 1
            while data[j] > p:
                j -= 1
            if i <= j:
                swap(data, i, j)
                i += 1
                j -= 1

        quicksort(data, low, j)
        quicksort(data, i, high)

# Utilizar el ordenamiento sin importar los parámetros
def quicksort_wrapper(arr): 
    data = arr[:]  # Crea una copia para no modificar la lista original
    quicksort(data, 0, len(data) - 1)
    return data

def swap(data, i, j):
    data[i], data[j] = data[j], data[i]