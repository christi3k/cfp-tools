import agate
import argparse
import helper
import cairosvg
import logging
import warnings
import pprint

from decimal import Decimal

# initialize argument parser
parser = argparse.ArgumentParser(description="Process csv files from Wufoo for CFP analytics.")
parser.add_argument("input_file", help="Input filename. Only .csv supported.")

# initilize logging
logging.basicConfig(level=logging.ERROR)

# attempt to supress annoying deprecation warnings
# that seem to be coming mostly from dependency:
# https://github.com/tiran/defusedxml/pull/24
warnings.simplefilter("ignore", category=DeprecationWarning)
warnings.simplefilter("ignore", category=RuntimeWarning)

pp = pprint.PrettyPrinter()

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

    # create Agate tablre from csv file
    proposals = agate.Table.from_csv(args.input_file)

    # proposals.print_structure()
    # exit()

    # ================================================================
    # Rename columns that would otherwise be awkward to work with
    # ================================================================
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
        'What is your level speaking experience?': 'Experience'
    }

    proposals = proposals.rename(rename_columns)

    # ================================================================
    # compute new values:
    # - drop timestamp from Date Created
    # - normalize 'Speaker Pronouns' responses
    # ================================================================
    proposals = proposals.compute([
        ('Day Created', agate.Formula(agate.Date(), helper.round_date)),
        ('Speaker Pronouns', agate.Formula(agate.Text(), lambda row: str(row['Speaker Pronouns']).lower())),
        ('Employee', agate.Formula(agate.Text(), helper.is_employee)),
        ('Speaker Company', agate.Formula(agate.Text(), helper.fix_hashicorp))
    ], replace=True)

    # ================================================================
    # Determine and print number of proposals
    # ================================================================
    proposal_count = len(proposals)
    print("Proposals submitted: " + str(proposal_count))

    # pivot(key=None, pivot=None, aggregation=None, computation=None, default_value=<object object>, key_name=None)

    # pivot = proposals.pivot('Day Created', aggregation=agate.Count('Entry Id'), computation=agate.Formula(agate.Number(), calc_running_total))

    # ================================================================
    # Calculate running total numnber of proposals received
    # ================================================================
    pivot = proposals.pivot('Day Created', aggregation=agate.Count('Entry Id'))
    pivot = pivot.compute([('Running Total', helper.RunningSum('Count'))])

    # pivot.print_table()
    # exit()
    
    print('\nRunning Total:\n')
    pivot.print_bars('Day Created', 'Running Total')
    pivot.line_chart('Day Created','Running Total', args.input_file + '-total-by-day.svg')
    cairosvg.svg2png(url=args.input_file+'-total-by-day.svg', write_to=args.input_file+'-total-by-day.png')
    pivot.to_csv(args.input_file + '-total-by-day.csv')

    # ================================================================
    # Generate matrix of proposals by product
    # ================================================================
    print('\nBy product (matrix):\n')
    products = proposals.pivot(['Terraform','Nomad','Packer','Vault','Vagrant','Sentinel','Consul'])
    products.print_table(max_rows=None, max_columns=None)
    products.to_csv(args.input_file + '-product-matrix.csv')

    # ================================================================
    # Generate proposal count per each product
    # (might be superceeded by product and level matrix below
    # ================================================================
    print('\nBy product:\n')
    terraform = proposals.aggregate(agate.Count(column_name='Terraform', value='Terraform'))
    nomad = proposals.aggregate(agate.Count(column_name='Nomad', value='Nomad'))
    vagrant = proposals.aggregate(agate.Count(column_name='Vagrant', value='Vagrant'))
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
    table.print_bars('Product', 'Proposal Count')
    table.bar_chart('Product', 'Proposal Count', args.input_file + '-by-product.svg')
    cairosvg.svg2png(url=args.input_file+'-by-product.svg', write_to=args.input_file+'-by-product.png')
    table.to_csv(args.input_file + '-by-product.csv')

    # ================================================================
    # Count by product and level
    # ================================================================
    print('\nBy product and level:\n')
    products = ['Consul','Terraform','Packer','Nomad','Vagrant','Sentinel','Vault']

    # we can't just use the dictionary before because of weirdness with Agate table.from_object
    compiled = [
        ['Beginner'],
        ['Intermediate'],
        ['Advanced']
    ]
    
    # ensure computed values for columns and rows aligns
    levels = {
            0:'Beginner',
            1:'Intermediate', 
            2:'Advanced'
    }

    # we know name of first column
    # remaining columns will have names of products and be set
    # as we loop through them
    column_names = ['Level']

    column_types = [
            agate.Text(), 
            agate.Number(),
            agate.Number(),
            agate.Number(),
            agate.Number(),
            agate.Number(),
            agate.Number(),
            agate.Number()
        ]

    for product in products:
        # get list of entries tagged with given product
        by_product = proposals.where(lambda row: row[product] == product)

        # pivot on level to get count of entries per product and level
        by_level_and_product = by_product.pivot('Level')
        # by_level_and_product.print_table()
        # by_level_and_product.print_bars('Level', 'Count')
        row_names = by_level_and_product.row_names
        # by_level_and_product.bar_chart('Level','Count', args.input_file + '-' + product +'-by-level.svg')
        # cairosvg.svg2png(url=args.input_file+'-'+product+'-by-level.svg', write_to=args.input_file+'-'+product+'-by-level.png')
        # table.to_csv(args.input_file +'-'+product+ '-by-level.csv')

        column_names.append(product)
        # fill out composite table
        for level_id, level in levels.items():
            if level in by_level_and_product.row_names:
                level_count = by_level_and_product.rows[level]['Count']
            else:
                level_count = 0
            compiled[level_id].append(level_count)

        # print(compiled)

    # table = agate.Table(compiled, column_names=['Level','Product'])
    level_product_matrix = agate.Table(compiled, column_names=column_names, column_types=column_types)
    level_product_matrix.print_table(max_rows=None, max_columns=None)

    level_product_matrix.to_csv(args.input_file +'-by-product_and_level.csv')

    # ================================================================
    # Proposal count by level of talk
    # ================================================================
    print('\nBy level of talk:\n')
    by_level = proposals.pivot('Level', aggregation=agate.Count('Entry Id'))
    by_level.print_bars('Level','Count')
    by_level.column_chart('Level','Count', args.input_file + '-by-level.svg')
    cairosvg.svg2png(url=args.input_file+'-by-level.svg', write_to=args.input_file+'-by-level.png')
    by_level.to_csv(args.input_file + '-by-level.csv')

    # ================================================================
    # by speaker pronouns
    # ================================================================
    print('\nSpeaker pronouns:\n')
    by_pronoun = proposals.pivot('Speaker Pronouns', aggregation=agate.Count('Entry Id'))
    # by_pronoun.print_table()
    by_pronoun.print_bars('Speaker Pronouns', 'Count')
    by_pronoun.bar_chart('Speaker Pronouns', 'Count', args.input_file + '-by-pronoun.svg')
    cairosvg.svg2png(url=args.input_file+'-by-pronoun.svg', write_to=args.input_file+'-by-pronoun.png')
    by_pronoun.to_csv(args.input_file + '-by-pronoun.csv')

    # ================================================================
    # by underrepresented groups
    # ================================================================
    print('\nUnderrepresented:\n\n')
    by_underrep = proposals.pivot('Underrepresented', aggregation=agate.Count('Entry Id'))
    by_underrep.print_bars('Underrepresented', 'Count')
    by_underrep.bar_chart('Underrepresented', 'Count', args.input_file + '-by-underrep.svg')
    cairosvg.svg2png(url=args.input_file+'-by-underrep.svg', write_to=args.input_file+'-by-underrep.png')
    by_underrep.to_csv(args.input_file + '-by-underrep.csv')

    # by underrepresented, percent
    by_underrep = proposals.pivot('Underrepresented', aggregation=agate.Count('Entry Id'), computation=agate.Percent('Count'))
    # by_underrep.print_table()
    by_underrep.print_bars('Underrepresented', 'Percent')
    by_underrep.bar_chart('Underrepresented', 'Percent', args.input_file + '-by-underrep-percent.svg')
    cairosvg.svg2png(url=args.input_file+'-by-underrep-percent.svg', write_to=args.input_file+'-by-underrep-percent.png')
    by_underrep.to_csv(args.input_file + '-by-underrep-percent.csv')

    # underrepresented groups
    by_underrep_groups = proposals.pivot('Underrep Groups', aggregation=agate.Count('Entry Id'))
    by_underrep_groups.print_bars('Underrep Groups', 'Count')
    by_underrep_groups.bar_chart('Underrep Groups', 'Count', args.input_file + '-by-underrep-groups.svg')
    cairosvg.svg2png(url=args.input_file+'-by-underrep-groups.svg', write_to=args.input_file+'-by-underrep-groups.png')
    by_underrep_groups.to_csv(args.input_file + '-by-underrep-groups.csv')

    # ================================================================
    # travel assistance
    # ================================================================
    print('\nTravel assistance:\n')
    by_travel = proposals.pivot('Travel Assist', aggregation=agate.Count('Entry Id'))
    by_travel.print_bars('Travel Assist', 'Count')
    by_travel.column_chart('Travel Assist', 'Count', args.input_file + '-by-travel.svg')
    cairosvg.svg2png(url=args.input_file+'-by-travel.svg', write_to=args.input_file+'-by-travel.png')
    by_travel.to_csv(args.input_file + '-by-travel.csv')

    # ================================================================
    # by speaking experience
    # ================================================================
    print('\nSpeaking experience:\n')
    by_experience = proposals.pivot('Experience', aggregation=agate.Count('Entry Id'))
    # by_experience.print_table()
    by_experience.print_bars('Experience','Count')
    by_experience.column_chart('Experience','Count', args.input_file + '-by-experience.svg')
    cairosvg.svg2png(url=args.input_file+'-by-experience.svg', write_to=args.input_file+'-by-experience.png')
    by_experience.to_csv(args.input_file + '-by-experience.csv')

    # experience = proposals.columns['Experience'].values_distinct()

    # ================================================================
    # By employee status
    # ================================================================
    print('\nBy employee status:\n')
    by_employee = proposals.pivot('Employee', aggregation=agate.Count('Entry Id'))
    by_employee.print_bars('Employee','Count')
    by_employee.column_chart('Employee','Count', args.input_file + '-by-employee.svg')
    cairosvg.svg2png(url=args.input_file+'-by-employee.svg', write_to=args.input_file+'-by-employee.png')
    by_employee.to_csv(args.input_file + '-by-employee.csv')

    # ================================================================
    # By company
    # ================================================================
    print('\nBy company:\n')
    by_company = proposals.pivot('Speaker Company', aggregation=agate.Count('Entry Id'))
    by_company.print_bars('Speaker Company','Count')
    by_company.column_chart('Speaker Company','Count', args.input_file + '-by-company.svg')
    cairosvg.svg2png(url=args.input_file+'-by-company.svg', write_to=args.input_file+'-by-company.png')
    by_company.to_csv(args.input_file + '-by-company.csv')

