#Created 2021-09-05
#Copyright Spencer W. Leifeld

class Maps:
    @staticmethod
    def IntToBool(element):
        if isinstance(element, int):
            return bool(element)
        else:
            return element
