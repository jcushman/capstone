import csv

headers = "OK,Court ID,Court Name,Fixed Court Name,Court Name Abbreviation,Fixed Court Name Abbreviation,Count,Jurisdiction,Fixed Jurisdiction,First Date,Last Date,Example Case ID,Example Case".split(',')
old_courts = list(csv.DictReader(open('old_courts.csv')))
new_courts = list(csv.DictReader(open('courts.csv'), "Court ID,Court Name,Court Name Abbreviation,Count,Jurisdiction,First Date,Last Date,Example Case ID".split(",")))

old_courts_lookup = {c['Court ID']: c for c in old_courts}

copy_keys = ['OK', 'Fixed Court Name', 'Fixed Court Name Abbreviation', 'Fixed Jurisdiction']
for court in new_courts:
    if court['Court ID'] in old_courts_lookup:
        old_court = old_courts_lookup[court['Court ID']]
        for key in copy_keys:
            court[key] = old_court[key]
    else:
        for key in copy_keys:
            court[key] = ''

new_courts.sort(key=lambda c: c['Court Name'])

with open('courts_fixed.csv', 'w') as out:
    writer = csv.DictWriter(out, headers)
    writer.writeheader()
    writer.writerows(new_courts)
