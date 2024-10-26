from google.cloud.storage import Client, transfer_manager
from typing import List
from minio import Minio
from minio.error import S3Error



class GoogleStorageOperator():
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


    def upload_blob(self, bucket_name, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""
        
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        generation_match_precondition = 0
        try:
            blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)
        except Exception as exc:
            raise Exception(exc)
        print(f"File {source_file_name} uploaded to {destination_blob_name}.")
        

    def upload_many_blobs(self, bucket_name, filenames, source_directory="./data", workers=8):
        """Upload every file in a list to a bucket, concurrently in a process pool.

        Each blob name is derived from the filename, not including the
        `source_directory` parameter. For complete control of the blob name for each
        file (and other aspects of individual blob metadata), use
        transfer_manager.upload_many() instead.
        """

        bucket = self.storage_client.bucket(bucket_name)

        results = transfer_manager.upload_many_from_filenames(bucket, filenames, source_directory=source_directory, max_workers=workers)

        for name, result in zip(filenames, results):
            # The results list is either `None` or an exception for each filename in
            # the input list, in order.

            if isinstance(result, Exception):
                print("Failed to upload {} due to exception: {}".format(name, result))
            else:
                print("Uploaded {} to {}.".format(name, bucket.name))
                
                
class MinioStorageOperator:
    def __init__(self, endpoint, access_key, secret_key, secure=False):
        """
        Khởi tạo kết nối với MinIO.
        
        :param endpoint: Địa chỉ máy chủ MinIO (host:port).
        :param access_key: Khóa truy cập MinIO.
        :param secret_key: Khóa bí mật MinIO.
        :param secure: Sử dụng HTTPS (mặc định là True).
        """
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure  # Đặt thành True nếu dùng HTTPS
        )

    def upload_file(self, bucket_name, object_name, file_path):
        """
        Upload tệp lên MinIO.

        :param bucket_name: Tên bucket trong MinIO.
        :param file_path: Đường dẫn đến tệp cần upload.
        :param object_name: Tên đối tượng sẽ lưu trên MinIO.
        """
        try:
            self.client.fput_object(bucket_name, object_name, file_path)
            print(f"Upload thành công: {object_name}")
        except S3Error as e:
            print(f"Lỗi khi upload tệp: {str(e)}")

    def download_file(self, bucket_name, object_name, download_path):
        """
        Download tệp từ MinIO về máy.

        :param bucket_name: Tên bucket trong MinIO.
        :param object_name: Tên đối tượng trên MinIO cần tải về.
        :param download_path: Đường dẫn lưu tệp tải về.
        """
        try:
            self.client.fget_object(bucket_name, object_name, download_path)
            print(f"Download thành công: {object_name}")
        except S3Error as e:
            print(f"Lỗi khi download tệp: {str(e)}")

    def create_bucket(self, bucket_name):
        """
        Tạo bucket trong MinIO nếu chưa tồn tại.

        :param bucket_name: Tên bucket cần tạo.
        """
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                print(f"Đã tạo bucket: {bucket_name}")
            else:
                print(f"Bucket {bucket_name} đã tồn tại.")
        except S3Error as e:
            print(f"Lỗi khi tạo bucket: {str(e)}")