import os

from mypytable import MyPyTable 

def main():
    mpg_fname = os.path.join("input_data", "auto-mpg.txt")
    prices_fname = os.path.join("input_data", "auto-prices.txt")
    print(mpg_fname, prices_fname)
    
if __name__ == "__main__":
    main()