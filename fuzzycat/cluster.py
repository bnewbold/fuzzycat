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

try:
    import orjson as json
except ImportError as exc:
    import json

class LineProcessor:
    """
    Process input linewise and cache the result based on the sha1 of the input.
    """
    def __init__(self, line_input=None, encoding='utf-8',
                 base=os.path.join(os.path.expanduser("~"), ".cache", "fuzzycat"),
                 key="default"):
        """
        The line_input is a callable, returning the next line to work on.
        """
        self.line_input = line_input or fileinput.input
        self.encoding = encoding
        self.base = base
        self.key = key

    def cache_id(self):
        sha1 = hashlib.sha1()
        sha1.update(bytes(self.key, encoding="utf-8"))
        sha1.update(bytes(inspect.getsource(self.run), encoding="utf-8"))
        return sha1.hexdigest()

    def cache_file(self):
        cid = self.cache_id()
        return os.path.join(self.base, cid[:2], cid[2:])

    def run(self):
        cf = self.cache_file()
        if os.path.exists(cf):
            return cf
        stats = collections.Counter()
        with tempfile.NamedTemporaryFile(delete=False, mode="w") as tf:
            for line in self.line_input():
                doc = json.loads(line)
                try:
                    id = doc["ident"]
                    title = doc["title"]
                    if not title:
                        stats["title-none"] += 1
                        continue
                    else:
                        title = title.replace("\t", " ").replace("\n", " ").strip()
                except KeyError as err:
                    stats["title-miss"] += 1
                    continue
                print("%s\t%s" % (id, title), file=tf)

        os.makedirs(os.path.dirname(cf))
        os.rename(tf.name, cf)
        return cf


def main():
    parser = argparse.ArgumentParser(prog='fuzzycat-cluster',
                                     usage='%(prog)s [options]',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-t", "--type", default="title", help="clustering variant to use")
    args = parser.parse_args()

    proc = LineProcessor(key="100k")
    cached = proc.run()
