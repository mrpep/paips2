def read_list(x):
    with open(x,'r') as f:
        y = f.read().splitlines()
    return y