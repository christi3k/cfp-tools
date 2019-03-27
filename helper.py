import agate
import re
from decimal import Decimal

def round_date(row):
    return row['Date Created'].strftime('%Y-%m-%d')

def is_employee(row):
    result = re.search(r'[Hh]ashi[Cc]orp', row['Speaker Company'])
    return False if result is None else True

def fix_hashicorp(row):
    return 'HashiCorp' if is_employee(row) else row['Speaker Company']

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

