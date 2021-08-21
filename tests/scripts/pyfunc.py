def operate(a,b,operation='sum'):
    if operation == 'sum':
        return a+b
    elif operation == 'mul':
        return a*b
    else:
        raise Exception('Unrecognized operation')