from google.cloud.storage import Client, transfer_manager
from typing import List



class CloudStorageHandler():
    def __init__(self) -> None:
        self.storage_client = Client()
        pass
    
    def set_bucket_public_iam(
        self,
        bucket_name:str="embedding-vectors",
        roles:List[dict]=[{"role": "roles/storage.objectViewer", "members": "allUsers"}]
    ):
        """Set a public IAM Policy to bucket"""
        # bucket_name = "your-bucket-name"
        
        bucket = self.storage_client.bucket(bucket_name)

        policy = bucket.get_iam_policy(requested_policy_version=3)
        policy.bindings += roles

        bucket.set_iam_policy(policy)
        print(f"Bucket {bucket.name} is now publicly readable")


    def upload_blob(
        self, bucket_name, source_file_name, destination_blob_name, content_type
    ):
        """Uploads a file to the bucket."""
        
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        generation_match_precondition = 0
        try:
            blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)
        except Exception as exc:
            raise Exception(exc)
        print(f"File {source_file_name} uploaded to {destination_blob_name}.")
        

    def upload_many_blobs(
        self, bucket_name, filenames, source_directory="./data", workers=8
    ):
        """Upload every file in a list to a bucket, concurrently in a process pool.

        Each blob name is derived from the filename, not including the
        `source_directory` parameter. For complete control of the blob name for each
        file (and other aspects of individual blob metadata), use
        transfer_manager.upload_many() instead.
        """

        bucket = self.storage_client.bucket(bucket_name)

        results = transfer_manager.upload_many_from_filenames(bucket, filenames, source_directory=source_directory, max_workers=workers, content_type=content_type)

        for name, result in zip(filenames, results):
            # The results list is either `None` or an exception for each filename in
            # the input list, in order.

            if isinstance(result, Exception):
                print("Failed to upload {} due to exception: {}".format(name, result))
            else:
                print("Uploaded {} to {}.".format(name, bucket.name))