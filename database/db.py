

import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors

import shared.config as cfg
import random
import string
import json

from azure.storage.queue import QueueService

import crypto.keys as keys

# ----------------------------------------------------------------------------------------------------------
# Prerequistes - 
# 
# 1. An Azure Cosmos account - 
#    https://azure.microsoft.com/en-us/documentation/articles/documentdb-create-account/
#
# 2. Microsoft Azure Cosmos PyPi package - 
#    https://pypi.python.org/pypi/azure-cosmos/
# ----------------------------------------------------------------------------------------------------------
# Sample - demonstrates the basic CRUD operations on a Collection resource for Azure Cosmos
# 
# 1. Query for Collection
#  
# 2. Create Collection
#    2.1 - Basic Create
#    2.2 - Create collection with custom IndexPolicy
#    2.3 - Create collection with offer throughput set
#    2.4 - Create collection with unique key
#
# 3. Manage Collection Offer Throughput
#    3.1 - Get Collection performance tier
#    3.2 - Change performance tier
#
# 4. Get a Collection by its Id property
#
# 5. List all Collection resources in a Database
#
# 6. Delete Collection
# ----------------------------------------------------------------------------------------------------------
# Note - 
# 
# Running this sample will create (and delete) multiple DocumentContainers on your account. 
# Each time a DocumentContainer is created the account will be billed for 1 hour of usage based on
# the performance tier of that account. 
# ----------------------------------------------------------------------------------------------------------

HOST = cfg.settings['HOST']
MASTER_KEY = cfg.settings['MASTER_KEY']
DATABASE_ID = cfg.settings['DATABASE_ID']
COLLECTION_ID = cfg.settings['COLLECTION_ID']


database_link = 'dbs/' + DATABASE_ID
collection_link = database_link + '/colls/' + COLLECTION_ID

class IDisposable(cosmos_client.CosmosClient):
    """ A context manager to automatically close an object with a close method
    in a with statement. """

    def __init__(self, obj):
        self.obj = obj

    def __enter__(self):
        return self.obj # bound to target

    def __exit__(self, exception_type, exception_val, trace):
        # extra cleanup in here
        self = None

class DatabaseManagement:
    @staticmethod
    def find_database(client, id):
        print('1. Query for Database')

        databases = list(client.QueryDatabases({
            "query": "SELECT * FROM r WHERE r.id=@id",
            "parameters": [
                { "name":"@id", "value": id }
            ]
        }))

        if len(databases) > 0:
            print('Database with id \'{0}\' was found'.format(id))
        else:
            print('No database with id \'{0}\' was found'. format(id))
        
    @staticmethod
    def create_database(client, id):
        print("\n2. Create Database")
        
        try:
            client.CreateDatabase({"id": id})
            print('Database with id \'{0}\' created'.format(id))

        except errors.HTTPFailure as e:
            if e.status_code == 409:
               print('A database with id \'{0}\' already exists'.format(id))
            else: 
                raise errors.HTTPFailure(e.status_code)               
    
    @staticmethod
    def read_database(client, id):
        print("\n3. Get a Database by id")

        try:
            # All Azure Cosmos resources are addressable via a link
            # This link is constructed from a combination of resource hierachy and 
            # the resource id. 
            # Eg. The link for database with an id of Foo would be dbs/Foo
            database_link = 'dbs/' + id

            database = client.ReadDatabase(database_link)
            print('Database with id \'{0}\' was found, it\'s _self is {1}'.format(id, database['_self']))

        except errors.HTTPFailure as e:
            if e.status_code == 404:
               print('A database with id \'{0}\' does not exist'.format(id))
            else: 
                raise errors.HTTPFailure(e.status_code)    

    @staticmethod
    def list_databases(client):
        print("\n4. List all Databases on an account")
        
        print('Databases:')
        
        databases = list(client.ReadDatabases())
        
        if not databases:
            return

        for database in databases:
            print(database['id'])          

    @staticmethod
    def delete_database(client, id):
        print("\n5. Delete Database")
        
        try:
           database_link = 'dbs/' + id
           client.DeleteDatabase(database_link)

           print('Database with id \'{0}\' was deleted'.format(id))

        except errors.HTTPFailure as e:
            if e.status_code == 404:
               print('A database with id \'{0}\' does not exist'.format(id))
            else: 
                raise errors.HTTPFailure(e.status_code)

class CollectionManagement:
    @staticmethod
    def find_Container(client, id):
        print('1. Query for Collection')

        collections = list(client.QueryContainers(
            database_link,
            {
                "query": "SELECT * FROM r WHERE r.id=@id",
                "parameters": [
                    { "name":"@id", "value": id }
                ]
            }
        ))

        if len(collections) > 0:
            print('Collection with id \'{0}\' was found'.format(id))
        else:
            print('No collection with id \'{0}\' was found'. format(id))
        
    @staticmethod
    def create_Container(client, id):
        """ Execute the most basic Create of collection. 
        This will create a collection with 400 RUs throughput and default indexing policy """

        print("\n2.1 Create Collection - Basic")
        
        try:
            client.CreateContainer(database_link, {"id": id})
            print('Collection with id \'{0}\' created'.format(id))

        except errors.HTTPFailure as e:
            if e.status_code == 409:
               print('A collection with id \'{0}\' already exists'.format(id))
            else: 
                raise errors.HTTPFailure(e.status_code)               

        print("\n2.2 Create Collection - With custom index policy")
        
        try:
            coll = {
                "id": "collection_custom_index_policy",
                "indexingPolicy": {
                    "indexingMode": "lazy",
                    "automatic": False
                }
            }

            collection = client.CreateContainer(database_link, coll)
            print('Collection with id \'{0}\' created'.format(collection['id']))
            print('IndexPolicy Mode - \'{0}\''.format(collection['indexingPolicy']['indexingMode']))
            print('IndexPolicy Automatic - \'{0}\''.format(collection['indexingPolicy']['automatic']))
            
        except errors.HTTPFailure as e:
            if e.status_code == 409:
               print('A collection with id \'{0}\' already exists'.format(collection['id']))
            else: 
                raise errors.HTTPFailure(e.status_code) 

        print("\n2.3 Create Collection - With custom offer throughput")

        try:
            coll = {"id": "collection_custom_throughput"}
            collection_options = { 'offerThroughput': 400 }
            collection = client.CreateContainer(database_link, coll, collection_options )
            print('Collection with id \'{0}\' created'.format(collection['id']))
            
        except errors.HTTPFailure as e:
            if e.status_code == 409:
               print('A collection with id \'{0}\' already exists'.format(collection['id']))
            else: 
                raise errors.HTTPFailure(e.status_code)

        print("\n2.4 Create Collection - With Unique keys")

        try:
            coll = {"id": "collection_unique_keys", 'uniqueKeyPolicy': {'uniqueKeys': [{'paths': ['/field1/field2', '/field3']}]}}
            collection_options = { 'offerThroughput': 400 }
            collection = client.CreateContainer(database_link, coll, collection_options )
            unique_key_paths = collection['uniqueKeyPolicy']['uniqueKeys'][0]['paths']
            print('Collection with id \'{0}\' created'.format(collection['id']))
            print('Unique Key Paths - \'{0}\', \'{1}\''.format(unique_key_paths[0], unique_key_paths[1]))
            
        except errors.HTTPFailure as e:
            if e.status_code == 409:
               print('A collection with id \'{0}\' already exists'.format(collection['id']))
            else: 
                raise errors.HTTPFailure(e.status_code)

        print("\n2.5 Create Collection - With Partition key")
        
        try:
            coll = {
                "id": "collection_partition_key",
                "partitionKey": {
                    "paths": [
                      "/field1"
                    ],
                    "kind": "Hash"
                }
            }

            collection = client.CreateContainer(database_link, coll)
            print('Collection with id \'{0}\' created'.format(collection['id']))
            
        except errors.HTTPFailure as e:
            if e.status_code == 409:
               print('A collection with id \'{0}\' already exists'.format(collection['id']))
            else: 
                raise errors.HTTPFailure(e.status_code) 

    @staticmethod
    def manage_offer_throughput(client, id):
        print("\n3.1 Get Collection Performance tier")
        
        #A Collection's Offer Throughput determines the performance throughput of a collection. 
        #A Collection is loosely coupled to Offer through the Offer's offerResourceId
        #Offer.offerResourceId == Collection._rid
        #Offer.resource == Collection._self
        
        try:
            # read the collection, so we can get its _self
            collection_link = database_link + '/colls/{0}'.format(id)
            collection = client.ReadContainer(collection_link)

            # now use its _self to query for Offers
            offer = list(client.QueryOffers('SELECT * FROM c WHERE c.resource = \'{0}\''.format(collection['_self'])))[0]
            
            print('Found Offer \'{0}\' for Collection \'{1}\' and its throughput is \'{2}\''.format(offer['id'], collection['_self'], offer['content']['offerThroughput']))

        except errors.HTTPFailure as e:
            if e.status_code == 404:
                print('A collection with id \'{0}\' does not exist'.format(id))
            else: 
                raise errors.HTTPFailure(e.status_code)

        print("\n3.2 Change Offer Throughput of Collection")
                           
        #The Offer Throughput of a collection controls the throughput allocated to the Collection
        #To increase (or decrease) the throughput of any Collection you need to adjust the Offer.content.offerThroughput
        #of the Offer record linked to the Collection
        
        #The following code shows how you can change Collection's throughput
        offer['content']['offerThroughput'] += 100
        offer = client.ReplaceOffer(offer['_self'], offer)

        print('Replaced Offer. Offer Throughput is now \'{0}\''.format(offer['content']['offerThroughput']))
                                
    @staticmethod
    def read_Container(client, id):
        print("\n4. Get a Collection by id")

        try:
            # All Azure Cosmos resources are addressable via a link
            # This link is constructed from a combination of resource hierachy and 
            # the resource id. 
            # Eg. The link for collection with an id of Bar in database Foo would be dbs/Foo/colls/Bar
            collection_link = database_link + '/colls/{0}'.format(id)

            collection = client.ReadContainer(collection_link)
            print('Collection with id \'{0}\' was found, it\'s _self is {1}'.format(collection['id'], collection['_self']))

        except errors.HTTPFailure as e:
            if e.status_code == 404:
               print('A collection with id \'{0}\' does not exist'.format(id))
            else: 
                raise errors.HTTPFailure(e.status_code)    
    
    @staticmethod
    def list_Containers(client):
        print("\n5. List all Collection in a Database")
        
        print('Collections:')
        
        collections = list(client.ReadContainers(database_link))
        
        if not collections:
            return

        for collection in collections:
            print(collection['id'])          
        
    @staticmethod
    def delete_Container(client, id):
        print("\n6. Delete Collection")
        
        try:
           collection_link = database_link + '/colls/{0}'.format(id)
           client.DeleteContainer(collection_link)

           print('Collection with id \'{0}\' was deleted'.format(id))

        except errors.HTTPFailure as e:
            if e.status_code == 404:
               print('A collection with id \'{0}\' does not exist'.format(id))
            else: 
                raise errors.HTTPFailure(e.status_code)   

class DocumentManagement:
    
    @staticmethod
    def CreateDocuments(client):
        print('Creating Documents')

        # Create a SalesOrder object. This object has nested properties and various types including numbers, DateTimes and strings.
        # This can be saved as JSON as is without converting into rows/columns.

        generate = DocumentManagement.GetQueue()
        client.CreateItem(collection_link, generate)

        # As your app evolves, let's say your object has a new schema. You can insert SalesOrderV2 objects without any 
        # changes to the database tier.
        # sales_order2 = DocumentManagement.GetQueue("SalesOrder2")
        # client.CreateItem(collection_link, sales_order2)

    @staticmethod
    def ReadDocument(client, doc_id):
        print('\n1.2 Reading Document by Id\n')

        # Note that Reads require a partition key to be spcified. This can be skipped if your collection is not
        # partitioned i.e. does not have a partition key definition during creation.
        doc_link = collection_link + '/docs/' + doc_id
        response = client.ReadItem(doc_link)

        print('Document read by Id {0}'.format(doc_id))
        print('Public Key: \n{0}'.format(response.get('key')))
        return response.get('key')

    @staticmethod
    def ReadDocuments(client):
        print('\n1.3 - Reading all documents in a collection\n')

        # NOTE: Use MaxItemCount on Options to control how many documents come back per trip to the server
        #       Important to handle throttles whenever you are doing operations such as this that might
        #       result in a 429 (throttled request)
        documentlist = list(client.ReadItems(collection_link, {'maxItemCount':10}))
        
        print('Found {0} documents'.format(documentlist.__len__()))
        
        for doc in documentlist:
            print('Document Id: {0}'.format(doc.get('id')))

    @staticmethod
    def randomString(stringLength=10):
        """Generate a random string of fixed length """
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(stringLength))

    @staticmethod
    def GetQueue():
        u =  keys.Key().generate_keys()
        keys.Key().save_key(DocumentManagement.randomString(), u['privateKey'])
        #print(str(u['publicKey'], 'utf-8'))
        queue_service = QueueService(account_name=cfg.settings['STORAGE_ACCOUNT'], account_key=cfg.settings['STORAGE_ACCOUNT_KEY'])
        
        queueName = DocumentManagement.randomString()

        created = queue_service.create_queue(queueName)
        data = None

        if created == True:
            data = {
                'key': str(u['publicKey'], 'utf-8'),
                'queue': queueName
            }
        return data
    