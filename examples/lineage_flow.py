# /// script
# requires-python = "==3.11.*"
# dependencies = [
#     "mlops-club-metaflow",
#     "setuptools"
# ]
#
# [tool.uv.sources]
# mlops-club-metaflow = { path = "..", editable = true }
# ///

from metaflow import FlowSpec, step

# Job: LineageFlow
#    Job: LineageFlow.start
#    Job: LineageFlow.end

class LineageFlow(FlowSpec):

    @step
    def start(self):
        print("Starting the flow...")
        self.next(self.end)

    @step
    def end(self):
        print("Flow completed.")

if __name__ == "__main__":
    LineageFlow()