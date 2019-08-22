from database.tables.init_tables import init_all_tables
from pipeline.orchestrater import PipelineOrchestrator


if __name__ == "__main__":
    init_all_tables()
    PipelineOrchestrator.execute()
