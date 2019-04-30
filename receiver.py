import crypto.hashing as hashing
import crypto.keys as keys
import users.user as user
import crypto.signature as signature
import crypto.encrypt as encrypt
import database.db as d
import json

import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors

def main():
    print("Running receiver")
    with d.IDisposable(cosmos_client.CosmosClient(d.HOST, {'masterKey': d.MASTER_KEY} )) as client:
        try:
            ''''''
            # setup database for this sample
            try:
                client.CreateDatabase({"id": d.DATABASE_ID})

            except errors.HTTPFailure as e:
                if e.status_code == 409:
                    pass
                else:
                    raise errors.HTTPFailure(e.status_code)

            # setup collection for this sample
            try:
                client.CreateContainer(d.database_link, {"id": d.COLLECTION_ID})
                print('Collection with id \'{0}\' created'.format(d.COLLECTION_ID))

            except errors.HTTPFailure as e:
                if e.status_code == 409:
                    print('Collection with id \'{0}\' was found'.format(d.COLLECTION_ID))
                else:
                    raise errors.HTTPFailure(e.status_code)
            
            # Create keys and queue 
            # d.DocumentManagement.CreateDocuments(client)

            data = d.DocumentManagement.ReadDocument(client,'0376af03-9620-4980-a905-dbfa6b189495')
            publicKey = keys.Key().read_key(data['publicKey'])
            privateKey = keys.Key().read_key_from_file('wxptlpygzv')
            queue = data['queue']

            print(publicKey)
            print(privateKey)
            print(queue)


        except errors.HTTPFailure as e:
            print('\nrun_sample has caught an error. {0}'.format(e._http_error_message))
        
        finally:
            print("\nrun_sample done")


if __name__ == '__main__':
    main()