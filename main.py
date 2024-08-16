import requests, os
from sqlglot import parse_one, exp
import typer
from neo4j import GraphDatabase
import urllib3

urllib3.disable_warnings()


app = typer.Typer()

host = os.environ.get('host') if os.environ.get('host') else 'http://localhost:3001'
neo4jURI = os.environ.get('neo4juri') if os.environ.get('neo4juri') else 'neo4j://localhost:7687'

login_url = f"{host}/api/session"
table_url = f"{host}/api/table"
card_url = f"{host}/api/card"
collections_url = f"{host}/api/collection"
databases_url = f"{host}/api/database"
table_url = f"{host}/api/table"
dashboard_url = f"{host}/api/dashboard"
native_card_url = f"{host}/api/dataset/native"

def metabaseAuth() -> any:
    USER = os.environ.get('user') if os.environ.get('user') else 'a@b.com'
    PASSWORD = os.environ.get('password') if os.environ.get('password') else 'metabot1'
    login_payload = {"username": f"{USER}", "password": f"{PASSWORD}"}
    session = requests.Session()
    session_cookie = os.environ.get('session_cookie') if os.environ.get('session_cookie') else ''
    if session_cookie:
        session.cookies.set('metabase.SESSION', f'{session_cookie}')
    else:
        session.post(login_url, json=login_payload, verify=False)
    return session

def dbAuth() -> any:
    driver = GraphDatabase.driver(neo4jURI)
    driver.verify_connectivity()
    # wipe everything before starting
    driver.execute_query('MATCH (n) DETACH DELETE n;')
    return driver

def item_generator(json_input, lookup_key):
    if isinstance(json_input, dict):
        for k, v in json_input.items():
            if k == lookup_key:
                yield v
            else:
                yield from item_generator(v, lookup_key)
    elif isinstance(json_input, list):
        for item in json_input:
            yield from item_generator(item, lookup_key)

def key_finder(source_dict:dict, key:str) -> []:
    output = []
    for value in item_generator(source_dict, key):
        output.append({key: value})
    return output

def getTableName(session, id:any) -> str:
    # table name can be an id or another Card, so that's why we check for the string type
    if isinstance(id, str) == True:
        return f"{id.capitalize()}"
    response = session.get(f"{table_url}/{id}")
    table_metadata = response.json()
    return f"{table_metadata['name']}"

def getCollectionMetadata(session, collection_id: any, **dashboards:bool) -> dict:
    # Collection can be 'Root' or an id, that's why it's an any type
    if dashboards:
        item_url = f"{host}/api/collection/{collection_id}/items?models=dashboard"
    else:
        item_url = f"{host}/api/collection/{collection_id}/items?models=dataset&models=card"
    response = session.get(item_url, verify=False)
    return response.json()

def getCollectionsMetadata(session, skip_archived:bool) -> dict:
    collections = []

    response = session.get(collections_url, verify=False)
    for collection in response.json():
        if skip_archived and 'archived' in collection and collection['archived']:
            continue
        slug = 'root' if collection['id'] == 'root' else collection['slug']
        collections.append({'id': collection['id'], 'name': collection['name'], 'slug': slug})
    return collections

# Snippets not yet supported, query with filters as well
def getSourcesFromCard(session, id:int) -> dict:
    card_sources = []
    response = session.get(f"{card_url}/{id}", verify=False)
    if (response.status_code == 200):
        card_metadata = response.json()
        # Is the query a native query?
        if 'native' in card_metadata['dataset_query']:
            # If it is, does it have template tags?
            if 'template-tags' in card_metadata['dataset_query']['native'] and len(card_metadata['dataset_query']['native']['template-tags']) != 0:
                # Then let's find the cards on those template tags and append them to the array
                cards = key_finder(card_metadata, 'card-id')
                for source in cards:
                    card_sources.append({'source-table' : f"card__{source['card-id']}"})
                # Now, let's grab the query and replace all those template tags for a dummy table, this is extremely important to parse the SQL afterwards, or otherwise it will fail
                for tag in card_metadata['dataset_query']['native']['template-tags']:
                    query_to_parse = card_metadata['dataset_query']['native']['query'].replace(f'{{{{{tag}}}}}', 'dummy').replace(f'{{{{{ tag }}}}}', 'dummy')
            else:
                query_to_parse = card_metadata['dataset_query']['native']['query']
            # And after all, wipe optional clauses
            query_to_parse = query_to_parse.replace('[[', '').replace(']]', '')
            try:
                for table in parse_one(query_to_parse).find_all(exp.Table):
                    if table.name != 'dummy':
                        card_sources.append({'sql-source-table': table.name})
            except Exception as e:
                print(f"Could't parse {id} {e}")
        else:
            card_sources = key_finder(card_metadata, 'source-table')
        card = {
            'card_sources': card_sources,
            'card_id': str(card_metadata['id']),
            'database': card_metadata['dataset_query']['database'],
            'card_name': card_metadata['name'],
            'collection_slug': 'root' if card_metadata['collection']['id'] == 'root' else card_metadata['collection']['slug'],
            'archived': card_metadata['archived']
            }
        return card
    else:
        return {
            "card_name": 'Corrupted card',
            "card_id": id,
            'collection_slug': 'root',
            'card_sources': [],
            'archived': True
            }

def getSchemas(session, database:int) -> dict:
    schemas_metadata = session.get(f"{databases_url}/{database}/schemas", verify=False)
    return schemas_metadata.json()

def getDashboardMetadata(session, dashboard:int) -> dict:
    response = session.get(f"{dashboard_url}/{dashboard}", verify=False)
    dashboard_metadata = response.json()
    return dashboard_metadata

def getDashboardCards(session, dashboard:dict) -> dict:
    response = session.get(f"{dashboard_url}/{dashboard}", verify=False)
    dashboard_metadata = response.json()
    return dashboard_metadata["dashcards"]

def getDatabases(session) -> dict:
    databases = {}
    databases_metadata = session.get(databases_url, verify=False)
    databases_metadata = databases_metadata.json()
    for database in databases_metadata["data"]:
        # H2 and Mongo are not supported. H2 simply because it's the sample database, Mongo due to the fact that it's a NoSQL database and it's not supported by SQLglot
        if database["engine"] == "h2" or database["engine"] == "mongo":
            continue
        databases.update({database["id"]: database["name"]})
    return databases

def getTables(session, database: int, schema:dict) -> dict:
    tables = {}
    if "/" in schema:
        schema = schema.replace("/", "%2F")
    tables_metadata = session.get(f"{databases_url}/{database}/schema/{schema}", verify=False)
    try:
        tables_metadata = tables_metadata.json()
    except Exception as e:
        print(e)
        print(database, schema)
    for table in tables_metadata:
        tables.update({table["id"]: table["name"]})
    return tables

def getFields(session, table: dict) -> dict:
    fields = {}
    fields_metadata = session.get(f"{table_url}/{table}/query_metadata", verify=False)
    fields_metadata = fields_metadata.json()
    for field in fields_metadata['fields']:
        fields.update({field['id']: field['name']})
    return fields

def sanitize(string:str) -> str:
    for ch in [' ', "'", "(", ")", "{", "}", "/", "\\", "-", "@", ".", "?", "[", "]", ":", ";", ",", "!", "#", "$", "%", "^", "&", "*", "+", "=", "<", ">", "|", "~", "`"]:
        string = string.replace(ch, "_")
    return string

def writeTo(writerType:str, writer, syntax:str) -> None:
    try:
        if writerType == 'cypher':
            writer.execute_query(syntax, database="neo4j")
        if writerType == 'file':
            return writer.write(syntax)
    except Exception as e:
        print(e)

def writeDatabases(session, writerType, writer, fields:bool, database_list:str) -> None:
    databases = getDatabases(session)
    # Writing databases, schemas, tables (and fields)
    with typer.progressbar(range(len(databases)), label="Writing databases") as progress:
        total = 0
        for database in databases:
            if database_list and database not in database_list:
                continue
            createDatabase = f"CREATE ({sanitize(databases[database])}{database}:Database {{name:'{sanitize(databases[database])}',key:'db{database}'}})\n"
            writeTo(writerType, writer, createDatabase)
            schemas = getSchemas(session, database)
            for schema in schemas:
                createSchema = f"CREATE ({sanitize(schema)}{database}:Schema {{name: '{schema}'}})\n"
                matchSchemaAndDb = f"MATCH (a_db:Database {{key:'db{database}'}}),(a_schema:Schema {{name:'{schema}'}})\n"
                createSchemaDependency = f"CREATE (a_db)-[:BELONGS_TO]->(a_schema)\n"
                writeTo(writerType, writer, createSchema)
                writeTo(writerType, writer, matchSchemaAndDb +createSchemaDependency)
                tables = getTables(session, database, schema)
                for table in tables:
                    createTable = f"CREATE ({sanitize(tables[table])}{table}:Table {{name: '{sanitize(tables[table])}', key: 'table{table}'}})\n"
                    matchTableAndSchema = f"MATCH (a_schema:Schema {{name:'{schema}'}}), (a_table:Table {{key:'table{table}'}})\n"
                    createTableDependency = f"CREATE (a_table)-[:BELONGS_TO]->(a_schema)\n"
                    writeTo(writerType, writer, createTable)
                    writeTo(writerType, writer, matchTableAndSchema + createTableDependency)
                    if fields:
                        fields = getFields(session, table)
                        for field in fields:
                            createField = f"CREATE ({sanitize(fields[field])}{field}:Field {{name:'{sanitize(fields[field])}', key:'field{field}'}})\n"
                            matchFieldAndTable = f"MATCH (a_table:Table {{key:'table{table}'}}), (a_field:Field {{key:'field{field}'}})\n"
                            createFieldDependency = f"CREATE (a_field)-[:BELONGS_TO]->(a_table)\n"
                            writeTo(writerType, writer, createField)
                            writeTo(writerType, writer, matchFieldAndTable + createFieldDependency)
            total += 1
            progress.update(total)

def writeCollectionsAndCards(session, writerType, writer, skip_archived:bool, database_list:str) -> None:
    collections = getCollectionsMetadata(session, skip_archived)
    with typer.progressbar(range(len(collections)), label="Writing collections") as progress:
        total = 0
        max = 0
        for collection in collections:
            cards_metadata = getCollectionMetadata(session, collection['id'])
            createCollection = f"CREATE ({sanitize(collection['name'])}{collection['id']}:Collection {{name: '{collection['slug']}', key: 'collection{collection['id']}'}})\n"
            writeTo(writerType, writer, createCollection)
            
            for card in cards_metadata["data"]:
                # let's see the last card we have so we can go later and create one by one in sequential order. This is to prevent cases like questions that are created last and moved to the root collection, which makes the dependency check harder than usual
                if card['id'] > max:
                    max = card['id']
            total += 1
            progress.update(total)
    with typer.progressbar(range(len(collections)), label="Writing cards") as progress:
        total = 0
        # Now we'll loop over all cards
        for card in range(max):
            card_metadata = getSourcesFromCard(session, card + 1)
            if skip_archived and 'archived' in card_metadata and card_metadata['archived']:
                continue
            if database_list and card_metadata['database'] not in database_list:
                continue
            createGUICard = f"CREATE (Card__{card_metadata['card_id']}:Card {{name: '{sanitize(card_metadata['card_name'])}', key: 'card{card_metadata['card_id']}'}})\n"
            matchCardAndCollection = f"MATCH (a_collection:Collection {{name: '{card_metadata['collection_slug']}'}}), (a_card:Card {{key: 'card{card_metadata['card_id']}'}})\n"
            createGUICardRelationshipToCollection = f"CREATE (a_card)-[:BELONGS_TO]->(a_collection)\n"
            writeTo(writerType, writer, createGUICard)
            writeTo(writerType, writer, matchCardAndCollection + createGUICardRelationshipToCollection)
            if len(card_metadata) >= 1:
                for source in card_metadata['card_sources']:
                    # This is a source a what comes from SQL queries (e.g. select * from people)
                    if 'sql-source-table' in source:
                        matchCardWithTable = f"MATCH (a_card:Card {{key: 'card{card_metadata['card_id']}'}}), (card_or_table:Table {{name: '{source['sql-source-table'].capitalize()}'}})\n"
                    else:
                        # Imagine now the case where the question has a question as its source
                        if type(source['source-table']) == str:
                            # This should be a previously created card
                            matchCardWithTable = f"MATCH (a_card:Card {{key: 'card{card_metadata['card_id']}'}}), (card_or_table:Card {{key: '{source['source-table'].lower().replace('__','')}'}})\n"
                            # createCardRelationshipToCardOrTable = f"CREATE (a_card)-[:SOURCE]->(card_or_table)\n"
                        else:
                            # And this should be a regular table
                            matchCardWithTable = f"MATCH (a_card:Card {{key: 'card{card_metadata['card_id']}'}}), (card_or_table:Table {{name: '{sanitize(getTableName(session, source['source-table']))}'}})\n"
                    createCardRelationshipToCardOrTable = f"CREATE (a_card)-[:SOURCE]->(card_or_table)\n"
                    writeTo(writerType, writer, matchCardWithTable + createCardRelationshipToCardOrTable)
            total += 1
            progress.update(total)

def writeDashboards(session, writerType, writer, skip_archived):
    collections = getCollectionsMetadata(session, skip_archived)
    total = 0
    with typer.progressbar(range(len(collections)), label="Writing dashboards") as progress:
        for collection in collections:
            dashboards_metadata = getCollectionMetadata(session, collection['id'], dashboards=True)
            for dashboard in dashboards_metadata["data"]:
                dashboard = getDashboardMetadata(session, dashboard['id'])
                if skip_archived and 'archived' in dashboard and dashboard["archived"]:
                    continue
                createDashboard = f"CREATE ({sanitize(dashboard['name'])}{dashboard['id']}:Dashboard {{name: '{sanitize(dashboard['name'])}', key: 'dashboard{dashboard['id']}'}})\n"
                writeTo(writerType, writer, createDashboard)
                dashboard_cards = getDashboardCards(session, dashboard["id"])
                for card in dashboard_cards:
                    if 'id' in card['card']:
                        matchCardWithDashboard = f"MATCH (a_dashboard:Dashboard {{key: 'dashboard{dashboard['id']}'}}), (a_card:Card {{key: 'card{card['card']['id']}'}})\n"
                    else:
                        matchCardWithDashboard = f"MATCH (a_dashboard:Dashboard {{key: 'dashboard{dashboard['id']}'}}), (a_card:Card {{key: 'card{card['id']}'}})\n"
                    createDashboardRelationshipToCard = f"CREATE (a_dashboard)-[:CONTAINS]->(a_card)\n"
                    writeTo(writerType, writer, matchCardWithDashboard + createDashboardRelationshipToCard)
            total += 1
            progress.update(total)

@app.command()
def cypher(fields: bool = False, skip_archived: bool = True, database_list: str = ""):
    database_list = [int(e) if e.isdigit() else e for e in database_list.split(',')]
    try:
        metabaseSession = metabaseAuth()
    except:
        metabaseSession.delete(login_url)
    
    with open('metadata.cypher', 'w') as writer:
        writeDatabases(metabaseSession, 'file', writer, fields, database_list)
        # Writing Collections and cards
        writeCollectionsAndCards(metabaseSession, 'file', writer, skip_archived, database_list)
        # This is a 3rd pass in order to be safe and avoid writing a dashboard node before the card has been written (e.g. cards that were moved to a latter collection but belong to a dashboard that's on a small ID)
        # Writing Dashboards
        writeDashboards(metabaseSession, 'file', writer, skip_archived)
    metabaseSession.delete(login_url)

@app.command()
def neo4j(fields: bool = False, skip_archived: bool = True, database_list: str = ""):
    database_list = [int(e) if e.isdigit() else e for e in database_list.split(',')]
    try:
        metabaseSession = metabaseAuth()
    except:
        metabaseSession.delete(login_url)
    try:
        writer = dbAuth()
    except Exception as e:
        writer.close()
        print(e)
    
    writeDatabases(metabaseSession, 'cypher', writer, fields, database_list)
    # Writing Collections and cards
    writeCollectionsAndCards(metabaseSession, 'cypher', writer, skip_archived, database_list)
    writeDashboards(metabaseSession, 'cypher', writer, skip_archived)
    writer.close()
    metabaseSession.delete(login_url)

@app.command()
def database(fields: bool = False, database_list: str = ""):
    database_list = [int(e) if e.isdigit() else e for e in database_list.split(',')]
    metabaseSession = metabaseAuth()
    with open('metadata.cypher', 'w') as writer:
        writeDatabases(metabaseSession, 'file', writer, fields, database_list)
    metabaseSession.delete(login_url)

if __name__ == "__main__":
    app()
