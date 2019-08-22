from pipeline.definitions import *


class Orchestrator:

    def __init__(self):
        self.pipeline_objs = Pipeline.instances

    def execute(self):
        for pipe_obj in self.pipeline_objs:
            #if pipe_obj.name == "OrderSummaryPipeline":
            print(f"\nLOG: ----------- Running Pipeline: {pipe_obj.name} ---------------")
            pipe_obj.execute()
        print(f"LOG: Done")


PipelineOrchestrator = Orchestrator()
