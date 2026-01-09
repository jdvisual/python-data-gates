from typing import Tuple
import pandas as pd


class BaseGate:
    """
    Base class for all data quality gates.

    A gate validates a dataset and separates it into:
      - valid records
      - quarantined records with reason codes
    """

    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Validate a dataframe.

        Returns:
            valid_df: records that passed validation
            quarantine_df: records that failed validation
        """
        raise NotImplementedError("Gate must implement validate()")

