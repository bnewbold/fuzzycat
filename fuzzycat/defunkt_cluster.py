"""
Clustering part of matching.

Input is a json lines of release entities, e.g. from a database dump.

Example:

    $ cat re.jsonl | fuzzycat-cluster --type title
"""

import argparse
import sys
import fileinput
import collections
import hashlib
import os
import tempfile
import inspect
import contextlib

try:
    import orjson as json
except ImportError as exc:
    import json

DEFAULT_CACHE_DIR = os.path.join(os.path.expanduser("~"), ".cache", "fuzzycat")



class AbstractLineProcessor:
    """
    Process input linewise and cache the result based on the sha1 of the input.
    Some artifacts are bigger and it is a bit laborious to keep track manually.
    """
    def __init__(self, line_input=None, encoding='utf-8',
                 base=DEFAULT_CACHE_DIR,
                 key="default"):
        """
        The line_input is a callable, returning the next line to work on (e.g.
        fileinput.input). Set the key as a general cache key, e.g. to the sha1
        of the raw input file to group derived artifacts.
        """
        self.line_input = line_input or fileinput.input
        self.encoding = encoding
        self.base = base
        self.key = key

    def cache_id(self, encoding="utf-8"):
        """
        If the input (given by key) changes, the cache id should also change.
        Similarly, when the source of run is changes.
        """
        sha1 = hashlib.sha1()
        sha1.update(bytes(self.key, encoding=encoding))
        sha1.update(bytes(inspect.getsource(self.run), encoding=encoding))
        return sha1.hexdigest()

    def cache_file(self):
        cid = self.cache_id()
        return os.path.join(self.base, cid[:2], cid[2:])

    def run(self):
        raise NotImplementedError

    @contextlib.contextmanager
    def output(self):
        with tempfile.NamedTemporaryFile(delete=False, mode="w") as tf:
            try:
                yield tf
            finally:
                cf = self.cache_file()
                if os.path.exists(cf):
                    return cf
                os.makedirs(os.path.dirname(cf), exist_ok=True)
                os.rename(tf.name, cf)

def memoize(f):
    def inner(*args, **kwargs):
        self = args[0]
        cf = self.cache_file()
        print(cf)
        if os.path.exists(cf):
            return cf
        f(*args, **kwargs)
        return cf

    return inner

class LineProcessor(AbstractLineProcessor):

    @memoize
    def run(self):
        with self.output() as f:
            for line in self.line_input():
                doc = json.loads(line)
                try:
                    id = doc["ident"]
                    title = doc["title"]
                    if not title:
                        continue
                    else:
                        title = title.replace("\t", " ").replace("\n", " ").strip()
                except KeyError as err:
                    continue
                print("%s\t%s" % (id, title), file=f)

def main():
    parser = argparse.ArgumentParser(prog='fuzzycat-cluster',
                                     usage='%(prog)s [options]',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-t", "--type", default="title", help="clustering variant to use")
    args = parser.parse_args()

    proc = LineProcessor(key="100K")
    cached = proc.run()
