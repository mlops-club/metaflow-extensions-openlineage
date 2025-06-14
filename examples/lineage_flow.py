from metaflow import FlowSpec, step
from openlineage_metaflow import openlineage, execute_sql

# Job: LineageFlow
#    Job: LineageFlow.start
#    Job: LineageFlow.end

class LineageFlow(FlowSpec):

    @openlineage
    @step
    def start(self):
        print("Starting the flow...")

        execute_sql("SELECT * FROM my_table", "snowflake")

        self.next(self.end)

    @openlineage
    @step
    def end(self):
        print("Flow completed.")

if __name__ == "__main__":
    LineageFlow()