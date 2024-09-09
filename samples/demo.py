from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter, Or, And

# Add a new document
db = firestore.Client()

def create_document(data:dict=None, document_name:str=None):
    doc_ref = db.collection(u'users').document(document_name)
    doc_ref.set(data)
    print("DocumentID: ", doc_ref.id)
    
def get_documents(collection_name:str):
    for doc in db.collection(collection_name).stream():
        data = doc.to_dict()
        print(data)
        
def filter_documnet(collection_name:str):
    doc_ref = db.collection(collection_name)
    docs = doc_ref.order_by('last', direction=firestore.Query.DESCENDING).start_at({'first': 'Tony'}).stream()
    
    for doc in docs:
        print(doc.to_dict())




if __name__ == '__main__':
    data = {
        u'first': u'Xony',
        u'last': u'Nguyen',
        u'born': 2002
    }
    # create_document(data)
    
    # get_documents('users')
    
    print(filter_documnet('users'))
    