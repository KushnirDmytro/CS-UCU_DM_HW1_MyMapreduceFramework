

def read_from_csv(filename, diapasone):
    pass


def read_from_txt(filename, diapasone):
    pass


def read_from_raw_txt(filename, diapasone=None):
    with open(filename, 'rt') as myfile:
        data = [line for line in myfile]
    return data
    #todo read in diapasone
    pass



def write_to_csv(filename, diapasone):
    pass

def write_to_txt(filename, diapasone):
    pass