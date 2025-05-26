CREATE TABLE `my-project.my-dataset.lineage_history` (
    job_id STRING,
    query STRING,
    sources ARRAY<STRING>,
    targets ARRAY<STRING>,
    project_id STRING
);


SELECT * FROM `bigquery-public-data.samples.wikipedia` LIMIT 10


CREATE TABLE `qwiklabs-gcp-01-ecaf1e6cd828.mydataset.table1` AS
SELECT * FROM `bigquery-public-data.samples.wikipedia` LIMIT 5



CREATE TABLE `qwiklabs-gcp-01-ecaf1e6cd828.mydataset.table2` AS
SELECT * FROM `qwiklabs-gcp-01-ecaf1e6cd828.mydataset.table1` LIMIT 3



def extract_lineage(query):
    lineage = {'sources': set(), 'targets': set()}
    parsed = sqlparse.parse(query)
    if not parsed:
        return lineage
    for statement in parsed:
        sql = str(statement).lower()
        # Trouver les tables après "from" ou "join"
        from_matches = re.findall(r'from\s+([`\w\.\-]+)', sql)
        join_matches = re.findall(r'join\s+([`\w\.\-]+)', sql)
        lineage['sources'].update(from_matches)
        lineage['sources'].update(join_matches)
        
        # Trouver les tables après "insert into" ou "create table"
        insert_matches = re.findall(r'insert\s+into\s+([`\w\.\-]+)', sql)
        create_matches = re.findall(r'create\s+table\s+([`\w\.\-]+)', sql)
        lineage['targets'].update(insert_matches)
        lineage['targets'].update(create_matches)
    
    # Extraire uniquement le dataset et la table (en retirant les backticks et les parties du projet)
    def extract_dataset_table(table):
        parts = table.split('.')
        if len(parts) == 3:  # format projet.dataset.table
            return f'{parts[1]}.{parts[2]}'
        elif len(parts) == 2:  # format dataset.table
            return f'{parts[0]}.{parts[1]}'
        return table

    # Appliquer le filtrage pour obtenir uniquement dataset.table
    lineage['sources'] = {extract_dataset_table(table) for table in lineage['sources']}
    lineage['targets'] = {extract_dataset_table(table) for table in lineage['targets']}
    
    return lineage
