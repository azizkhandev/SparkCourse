names = [1, 2, 3]

def myfunc(num):
    if num in names:
        print("{} exists in the list provided!".format(num))
    else:
        print("{} does not exist in the list provided!".format(num))

def main():
    myfunc(10)

if __name__ == '__main__':
    main()

