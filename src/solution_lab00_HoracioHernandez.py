def solution(n):
    n.sort()
    for i in range(len(n)-1):
        diff = n[i+1] - n[i]
        if diff > 1:
            return n[i] + 1
    return len(n)

### A slighty different approach that I thought of later.
    #n.sort()
    #for i in range(len(n)):
    #   if i != n[i]:
    #       return i
    #return len(n)
