import dill
import atexit
from collections import defaultdict


def _cleanup(cache):
    with open('pipeline/cache.pickle', 'wb') as cache_file:
        dill.dump(cache, cache_file)


class Registry:

    """"wrapper around a dict, representing in memory caching to track delivered files,
    basically a dictionary that persists information when program finishes executing"""

    def __init__(self):
        self.cache = None

        try:
            with open('pipeline/cache.pickle', 'rb') as cache_file:
                self.cache = dill.load(cache_file)

        except (EOFError, FileNotFoundError):
            self.cache = defaultdict(lambda: defaultdict(lambda: set()))

        atexit.register(_cleanup, self.cache)

    def __getitem__(self, item):
        idx1, idx2, = item
        return self.cache[idx1][idx2]

    def register_key(self, pipeline, context, key):
        self.cache[pipeline][context].add(key)

    def remove_key(self, pipeline, context, key):
        self.cache[pipeline][context].remove(key)

    def exists(self, pipeline, context, key):
        return key in self.cache[pipeline][context]

    def retrieve_keys(self, pipeline, context):
        return list(self.cache[pipeline][context])


registry = Registry()
