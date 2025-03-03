# saver.py
from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod


class Saver(ABC):
    """
    Abstract base class for saving data in chunks.
    """

    def __init__(self, suppress_exceptions = False):
        self.executor = None
        self.first_chunk = True
        self.suppress_exceptions = suppress_exceptions

    def __enter__(self):
        """
        Enter the runtime context and initialize the executor.
        """
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.first_chunk = True
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        """
        Exit the runtime context and perform cleanup.
        """
        if self.executor:
            self.executor.shutdown(wait=True)
        if self.suppress_exceptions:
            if exception_type:
                print(f"An exception occurred: {exception_value}")
            return True  # Suppress exceptions if any
        else:
            return False

    @abstractmethod
    def save_impl(self, df):
        """
        Save the DataFrame. This method must be implemented by subclasses.

        :param df: DataFrame to save.
        """
        pass

    def async_save(self, df):
        """
        Save the DataFrame asynchronously.

        :param df: DataFrame to save.
        """
        self.executor.submit(self.save, df)

    def save(self, df):
        """
        Save the DataFrame and update the first_chunk flag.

        :param df: DataFrame to save.
        """
        self.save_impl(df)
        self.first_chunk = False