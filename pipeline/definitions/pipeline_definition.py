from functools import reduce
import pandas

class Pipeline:
    instances = []

    def __init__(self, name, table, steps):
        self.__class__.instances.append(self)
        self.name = name
        self.table = table
        self.steps = steps

    def execute(self):
        context = None
        for step in self.steps:
            context = step.execute(context)
            if isinstance(context, pandas.DataFrame):
                    if context.empty:
                        break
            elif not context:
                break
