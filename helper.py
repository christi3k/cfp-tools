import agate
from decimal import Decimal

def round_date(row):
    return row['Date Created'].strftime('%Y-%m-%d')

def calc_running_total(row):
    return 4
    # return row['Running Total'] + row['Count']


class RunningSum(agate.Computation):
    def __init__(self, column_name):
        self.column_name = column_name

    def get_computed_data_type(self, table):
        return agate.Number()

    def run(self, table):
        so_far = Decimal()
        sums = []
        for value in table.columns[self.column_name]:
            so_far += value
            sums.append(so_far)
        return sums

