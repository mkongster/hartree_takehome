import apache_beam as beam

from apache_beam import dataframe
from apache_beam.dataframe.convert import to_dataframe

from apache_beam.dataframe.io import read_csv

with beam.Pipeline() as p:
    dataset1 = (
        p | "Dataset1" >> read_csv('dataset1.csv')
    )
    dataset2 = (
        p | "Dataset2" >> read_csv('dataset2.csv')
    )


dataset = dataset1.merge(dataset2, on='counter_party')
max_rating = dataset.groupby(['legal_entity', 'counter_party', 'tier'])['rating'].agg('max').reset_index(name='max_rating')

dataset['value'] = dataset['value'].astype(int)
sum_arap = dataset.groupby(['legal_entity', 'counter_party', 'tier']).apply(lambda x: x[x['status'] == 'ARAP']['value'].sum()).reset_index(name='sum(status=ARAP)')

sum_accr = dataset.groupby(['legal_entity', 'counter_party', 'tier']).apply(lambda x: x[x['status'] == 'ACCR']['value'].sum()).reset_index(name='sum(status=ACCR)')

total = dataset.groupby(['legal_entity', 'counter_party', 'tier'])['value'].sum().reset_index(name='total_value')

aggregate = max_rating.merge(sum_arap, on=['legal_entity', 'counter_party', 'tier'])
aggregate = aggregate.merge(sum_accr, on=['legal_entity', 'counter_party', 'tier'])
aggregate = aggregate.merge(total, on=['legal_entity', 'counter_party', 'tier'])
aggregate.to_csv('output')
