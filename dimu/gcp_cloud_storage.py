from io import BytesIO

from tqdm.auto import tqdm
from google.cloud import storage
from pdf2image import convert_from_bytes


def convert_pdfs_to_pngs(s_bkt, s_path, d_bkt, d_path, zpad: int = 4):
    storage_client = storage.Client()
    bucket = storage_client.bucket(s_bkt)
    d_bucket = storage_client.bucket(d_bkt)
    blobs = bucket.list_blobs(prefix=s_path)
    blobs = [b for b in blobs]
    for b in tqdm(blobs):
        try:
            fname = b.name
            if fname.lower().endswith(".pdf"):
                _bytes = b.download_as_bytes()
                images = convert_from_bytes(_bytes)
                fname = fname[:-4]
                for i, img in enumerate(images):
                    suffix = str(i).zfill(zpad)
                    ofname = d_path + fname + "-" + suffix + ".png"
                    oblob = d_bucket.blob(ofname)
                    oblob.content_type="image/png"
                    with BytesIO() as f:
                        img.save(fp=f, format="png")
                        f.seek(0)
                        oblob.upload_from_file(file_obj=f)
        except:
            pass


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your GCS object
    # source_blob_name = "storage-object-name"

    # The path to which the file should be downloaded
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    # Construct a client side representation of a blob.
    # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
    # any content from Google Cloud Storage. As we don't need additional data,
    # using `Bucket.blob` is preferred here.
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Downloaded storage object {} from bucket {} to local file {}.".format(
            source_blob_name, bucket_name, destination_file_name
        )
    )


def list_blobs_with_prefix(bucket_name, prefix=None, delimiter=None):
    """Lists all the blobs in the bucket that begin with the prefix.

    This can be used to list all blobs in a "folder", e.g. "public/".

    The delimiter argument can be used to restrict the results to only the
    "files" in the given "folder". Without the delimiter, the entire tree under
    the prefix is returned. For example, given these blobs:

        a/1.txt
        a/b/2.txt

    If you specify prefix ='a/', without a delimiter, you'll get back:

        a/1.txt
        a/b/2.txt

    However, if you specify prefix='a/' and delimiter='/', you'll get back
    only the file directly under 'a/':

        a/1.txt

    As part of the response, you'll also get back a blobs.prefixes entity
    that lists the "subfolders" under `a/`:

        a/b/
    """

    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

    print("Blobs:")
    for blob in blobs:
        print(blob.name)

    if delimiter:
        print("Prefixes:")
        for prefix in blobs.prefixes:
            print(prefix)
