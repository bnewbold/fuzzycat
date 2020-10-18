"""
Clustering part of matching.
"""

import argparse


def main():
    parser = argparse.ArgumentParser(prog='fuzzycat-cluster',
                                     usage='%(prog)s [options]',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-t", "--type", default="title", help="clustering algorithm to use")

    args = parser.parse_args()

    print(args)
