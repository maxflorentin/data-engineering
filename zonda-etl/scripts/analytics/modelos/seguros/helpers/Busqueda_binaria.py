
class Node(object):
    def __init__(self, start, end ,l ,r):
        self.start = start
        self.end = end
        self.left = None
        self.right = None
        self.l_num = l
        self.r_num = r

    def busqueda(self ,valor):

        # CASOS BASE

        # Unica hoja de padre
        if ((self.l_num + 1) == self.r_num):
            return self.l_num

        # Hoja balanceada
        if ((self.l_num + 2) >= self.r_num):
            # caso base
            if self.left.end > valor:
                return self.left.l_num
            else:
                return self.right.l_num

        # RECORRER EL ARBOL
        elif (self.left.end > valor):
            return self.left.busqueda(valor)
        else:
            return self.right.busqueda(valor)


class NumArray(object):
    def __init__(self, nums):
        """
        initialize your data structure here.
        :type nums: List[int]
        """

        # helper function to create the tree from input array
        def createTree(nums, l, r):
            # base case
            # if l == r:
            #    return None

            # leaf node
            if ((l + 1) >= r):
                n = Node(nums[l], nums[r], l, r)

                return n

            mid = (l + r) // 2

            root = Node(nums[l], nums[r], l, r)

            # recursively build the Segment tree
            root.left = createTree(nums, l, mid)
            root.right = createTree(nums, mid, r)

            return root

        self.root = createTree(nums, 0, len(nums) - 1)


def asigna_cuantil_tree(valor, arbol):
    return arbol.root.busqueda(valor)