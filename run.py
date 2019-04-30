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
    '''
    h = hashing.Hash()
    k = keys.Key()
    s = signature.Signature()
    e = encrypt.Encrypt()

    a = k.generate_keys()
    b = k.generate_keys()

    alice = user.User(a['publicKey'], a['privateKey'])
    bob = user.User(b['publicKey'], b['privateKey'])
    '''


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
            


            #d.DocumentManagement.CreateDocuments(client)
            
            #key = d.DocumentManagement.ReadDocument(client,'9d88e4ce-3fe8-49e2-9c8f-446530299c13')
            #k = keys.Key().read_key(key)
            #print(k)

            #d.DocumentManagement.ReadDocuments(client)

        except errors.HTTPFailure as e:
            print('\nrun_sample has caught an error. {0}'.format(e._http_error_message))
        
        finally:
            print("\nrun_sample done")


if __name__ == '__main__':
    main()