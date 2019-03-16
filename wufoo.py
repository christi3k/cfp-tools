import agate
import argparse
import helper
import cairosvg

from decimal import Decimal


# initialize argument parser
parser = argparse.ArgumentParser(description="Process csv files from Wufoo for CFP analytics.")
parser.add_argument("input_file", help="Input filename. Only .csv supported.")


if __name__ == '__main__':

    args = parser.parse_args()

    import_file = args.input_file

    # specified_types = {
        # 'column_name_one': agate.Text(),
        # 'column_name_two': agate.Number()
    # }

    # table = agate.Table.from_csv('filename.csv', column_types=specified_types)


    # columns = ('value',)
    # rows = ([2],[4],[9],[9])
    # new_table = agate.Table(rows, columns)

    # new_table = new_table.compute([
        # ('percent', agate.Percent('value'))
    # ])

    # new_table.print_table()
    # exit()


    proposals = agate.Table.from_csv(args.input_file)

    rename_columns = {
        'HashiCorp Products': 'Consul',
        'HashiCorp Products_2': 'Nomad',
        'HashiCorp Products_3': 'Packer',
        'HashiCorp Products_4': 'Terraform',
        'HashiCorp Products_5': 'Vagrant',
        'HashiCorp Products_6': 'Vault',
        'HashiCorp Products_7': 'Sentinel',
        'Are you a member of any groups underrepresented in the tech industry?': 'Underrepresented',
        'Which group(s)?': 'Underrep Groups',
        'Travel assistance needed?': 'Travel Assist',
    }

    proposals = proposals.rename(rename_columns)

    # proposals = proposals.select(['Consul','Nomad','Packer','Terraform','Vagrant','Vault','Sentinel'])
    # proposals.print_table(max_rows=None, max_columns=None)

    proposals = proposals.compute([
        ('Day Created', agate.Formula(agate.Date(), helper.round_date))
    ])

    proposal_count = len(proposals)
    print("Proposals submitted: " + str(proposal_count))
    print("\n")
    # __import__('pprint').pprint(proposal_count)

    # proposals.print_structure()
    # exit()

    # proposals.to_csv(args.input_file + 'processed.csv')
    # exit()
    # pivot(key=None, pivot=None, aggregation=None, computation=None, default_value=<object object>, key_name=None)

    # pivot = proposals.pivot('Day Created', aggregation=agate.Count('Speaker Email'), computation=agate.Formula(agate.Number(), calc_running_total))
    pivot = proposals.pivot('Day Created', aggregation=agate.Count('Speaker Email'))
    pivot = pivot.compute([('Running Total', helper.RunningSum('Count'))])

    pivot.line_chart('Day Created','Running Total', args.input_file + '-total-by-day.svg')
    cairosvg.svg2png(url=args.input_file+'-total-by-day.svg', write_to=args.input_file+'-total-by-day.png')
    pivot.to_csv(args.input_file + '-total-by-day.csv')

    # pivot.print_table()
    # pivot.print_bars('Day Created')
    pivot.print_bars('Day Created', 'Running Total')


    # counts.order_by('exonerated').line_chart('exonerated', 'count', 'docs/images/line_chart.svg')



    # exit()
    products = proposals.pivot(['Terraform','Nomad','Packer','Vault','Vagrant','Sentinel','Consul'])
    # products.print_table(max_rows=None, max_columns=None)
    # __import__('pprint').pprint(set(tag_inventory))

    terraform = proposals.aggregate(agate.Count(column_name='Terraform', value='Terraform'))

    nomad = proposals.aggregate(agate.Count(column_name='Nomad', value='Nomad'))

    vagrant = proposals.aggregate(agate.Count(column_name='Vagrant', value='Vagrant'))
    # __import__('pprint').pprint(vagrant)

    vault = proposals.aggregate(agate.Count(column_name='Vault', value='Vault'))

    sentinel = proposals.aggregate(agate.Count(column_name='Sentinel', value='Sentinel'))

    consul = proposals.aggregate(agate.Count(column_name='Consul', value='Consul'))

    packer = proposals.aggregate(agate.Count(column_name='Packer', value='Packer'))

    column_names = ['Product', 'Proposal Count']
    column_types = [agate.Text(), agate.Number()]

    rows = [
        ('Consul', consul),
        ('Packer', packer),
        ('Nomad', nomad),
        ('Terraform', terraform),
        ('Sentinel', sentinel),
        ('Vagrant', vagrant),
        ('Vault', vault),
    ]

    table = agate.Table(rows, column_names, column_types)
    # table.print_table()
    table.print_bars('Product', 'Proposal Count')
    table.bar_chart('Product', 'Proposal Count', args.input_file + '-by-product.svg')
    cairosvg.svg2png(url=args.input_file+'-by-product.svg', write_to=args.input_file+'-by-product.png')
    table.to_csv(args.input_file + '-by-product.csv')

    by_level = proposals.pivot('Level', aggregation=agate.Count('Speaker Email'))
    # by_level.print_table()
    by_level.print_bars('Level','Count')
    by_level.column_chart('Level','Count', args.input_file + '-by-level.svg')
    cairosvg.svg2png(url=args.input_file+'-by-level.svg', write_to=args.input_file+'-by-level.png')
    by_level.to_csv(args.input_file + '-by-level.csv')

    # by pronoun
    by_pronoun = proposals.pivot('Speaker Pronouns', aggregation=agate.Count('Speaker Email'))
    # by_pronoun.print_table()
    by_pronoun.print_bars('Speaker Pronouns', 'Count')
    by_pronoun.bar_chart('Speaker Pronouns', 'Count', args.input_file + '-by-pronoun.svg')
    cairosvg.svg2png(url=args.input_file+'-by-pronoun.svg', write_to=args.input_file+'-by-pronoun.png')
    by_pronoun.to_csv(args.input_file + '-by-pronoun.csv')

    # by underrepresented
    by_underrep = proposals.pivot('Underrepresented', aggregation=agate.Count('Speaker Email'))
    by_underrep.print_bars('Underrepresented', 'Count')
    by_underrep.bar_chart('Underrepresented', 'Count', args.input_file + '-by-underrep.svg')
    cairosvg.svg2png(url=args.input_file+'-by-underrep.svg', write_to=args.input_file+'-by-underrep.png')
    by_underrep.to_csv(args.input_file + '-by-underrep.csv')

    # by underrepresented, percent
    by_underrep = proposals.pivot('Underrepresented', aggregation=agate.Count('Speaker Email'), computation=agate.Percent('Count'))
    # by_underrep.print_table()
    by_underrep.print_bars('Underrepresented', 'Percent')
    by_underrep.bar_chart('Underrepresented', 'Percent', args.input_file + '-by-underrep-percent.svg')
    cairosvg.svg2png(url=args.input_file+'-by-underrep-percent.svg', write_to=args.input_file+'-by-underrep-percent.png')
    by_underrep.to_csv(args.input_file + '-by-underrep-percent.csv')

    # by underrepresented groups
    by_underrep_groups = proposals.pivot('Underrep Groups', aggregation=agate.Count('Speaker Email'))
    by_underrep_groups.print_bars('Underrep Groups', 'Count')
    by_underrep_groups.bar_chart('Underrep Groups', 'Count', args.input_file + '-by-underrep-groups.svg')
    cairosvg.svg2png(url=args.input_file+'-by-underrep-groups.svg', write_to=args.input_file+'-by-underrep-groups.png')
    by_underrep_groups.to_csv(args.input_file + '-by-underrep-groups.csv')

    # by travel assist
    by_travel = proposals.pivot('Travel Assist', aggregation=agate.Count('Speaker Email'))
    by_travel.print_bars('Travel Assist', 'Count')
    by_travel.column_chart('Travel Assist', 'Count', args.input_file + '-by-travel.svg')
    cairosvg.svg2png(url=args.input_file+'-by-travel.svg', write_to=args.input_file+'-by-travel.png')
    by_travel.to_csv(args.input_file + '-by-travel.csv')

    # groups = proposals.group_by('Terraform')
    # counts = groups.aggregate([
        # ('proposal_count', agate.Count())
    # ])

    # counts.print_table()

    # terraform = proposals.where(lambda row: row['Terraform'] == 'Terraform')
    # # .aggregate(['Count', agate.Count()])

    # terraform.print_table()
    
