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
    
    import requests

    url = "https://api.themoviedb.org/3/trending/all/day?language=en-US"

    headers = {
        "accept": "application/json",
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJmMzE0OGE4OWJhZTczOGQ1ZDRiYzVkMGYwYzllODI3MyIsIm5iZiI6MTcyNjE4ODcyOS4xMTE0MzQsInN1YiI6IjY1ZDIxYzcxNmVlY2VlMDE4YTM5MmZkNyIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.iK4TL7F8nDMkFCcO4IJAgiap8hWbm7fHHfjP3csyBlk"
    }

    response = requests.get(url, headers=headers)

    print(response.text)
    
    