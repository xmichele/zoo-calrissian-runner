from abc import ABC, abstractmethod


class ExecutionHandler(ABC):
    @abstractmethod
    def get_secrets(self):
        pass

    @abstractmethod
    def get_pod_env_vars(self):
        pass

    @abstractmethod
    def get_pod_node_selector(self):
        pass

    @abstractmethod
    def handle_log(self, execution_log):
        pass

    @abstractmethod
    def handle_output(self, output):
        pass

    @abstractmethod
    def handle_usage_report(self, usage_report):
        pass
