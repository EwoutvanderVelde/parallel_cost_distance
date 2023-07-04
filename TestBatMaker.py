import os
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--folder", help="Cost raster file", type=str, default=f"{os.getcwd()}/demo")
    args = parser.parse_args()
    print(args)
    print(os.listdir(args.folder))
    return


if __name__ == "__main__":
    main()
