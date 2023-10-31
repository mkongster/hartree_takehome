import pandas

def read_data():
    dataset1 = pandas.read_csv('dataset1.csv')
    dataset2 = pandas.read_csv('dataset2.csv')
    dataset = dataset1.merge(dataset2, on='counter_party')
    dataset['value'] = dataset['value'].astype(int)
    
    return dataset


def perform_aggregation(dataset):
    max_rating = dataset.groupby(['legal_entity', 'counter_party', 'tier'])['rating'].agg('max').reset_index(name='max_rating')

    #get sum(value where status=ARAP)
    dataset['value'] = dataset['value'].astype(int)
    sum_arap = dataset.groupby(['legal_entity', 'counter_party', 'tier']).apply(lambda x: x[x['status'] == 'ARAP']['value'].sum()).reset_index(name='sum(status=ARAP)')

    #get sum(value where status=ACCR)
    sum_accr = dataset.groupby(['legal_entity', 'counter_party', 'tier']).apply(lambda x: x[x['status'] == 'ACCR']['value'].sum()).reset_index(name='sum(status=ACCR)')

    #get total per legal entity, counterparty & tier
    total = dataset.groupby(['legal_entity', 'counter_party', 'tier'])['value'].sum().reset_index(name='total_value')

    aggregate = max_rating.merge(sum_arap, on=['legal_entity', 'counter_party', 'tier'])
    aggregate = aggregate.merge(sum_accr, on=['legal_entity', 'counter_party', 'tier'])
    aggregate = aggregate.merge(total, on=['legal_entity', 'counter_party', 'tier'])
    return aggregate


def main():
    print(perform_aggregation(read_data()))


if __name__ == "__main__":
    main()