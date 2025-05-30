
class ShellSort:
    @staticmethod
    def generate_3smooth_gaps(n):
        gaps = set()
        p = 1
        while p < n:
            q = p
            while q < n:
                gaps.add(q)
                q *= 3
            p *= 2
        return sorted(gaps, reverse=True)

    @staticmethod
    def sort(arr):
        n = len(arr)
        gaps = ShellSort.generate_3smooth_gaps(n)
        for gap in gaps:
            for i in range(gap, n):
                temp = arr[i]
                j = i
                while j >= gap and arr[j - gap] > temp:
                    arr[j] = arr[j - gap]
                    j -= gap
                arr[j] = temp
        return arr  # Devolvemos el arreglo ordenado por conveniencia


# Utilizar el ordenamiento sin importar los parámetros
def shellsort_wrapper(arr):
    return ShellSort.sort(arr)
    