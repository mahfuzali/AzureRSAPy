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
import shared.config as cfg
from azure.storage.queue import QueueService


def main():
    print("Running sender")
    h = hashing.Hash()
    k = keys.Key()
    s = signature.Signature()
    enc = encrypt.Encrypt()

    with d.IDisposable(cosmos_client.CosmosClient(d.HOST, {'masterKey': d.MASTER_KEY} )) as client:
        try:
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
            
            data = d.DocumentManagement.ReadDocument(client,'8a83dd53-8ebc-443e-8af9-2e6c86cd01d5')
            publicKey = keys.Key().read_key(data['publicKey'])
            privateKey = keys.Key().read_key_from_file('nqzlqrczuv.pem')
            queue = data['queue']

            print(publicKey)
            print(privateKey)
            print(queue)

            print("Here 1")
            queue_service = QueueService(account_name=cfg.settings['STORAGE_ACCOUNT'], account_key=cfg.settings['STORAGE_ACCOUNT_KEY'])
            
            receiverInfo = d.DocumentManagement.ReadDocument(client,'0376af03-9620-4980-a905-dbfa6b189495')
            message = 'Hello World'
            print("Here 2")
            hsh = h.hash_string(message) 
            sign = s.sign(hsh, privateKey)
            #cipher = enc.encrypt(receiverInfo['publicKey'], message)
            
            print("Here 3")
            print(hashmsg)
            print(sign)
            #print(cipher)
            #queue_service.put_message(receiverInfo['queue'], cipher)
            #queue_service.put_message(receiverInfo['queue'], sign)

        except errors.HTTPFailure as e:
            print('\nrun_sample has caught an error. {0}'.format(e._http_error_message))
        
        finally:
            print("\nrun_sample done")


if __name__ == '__main__':
    main()