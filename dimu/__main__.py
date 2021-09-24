from fire import Fire

from dimu.gcp_cloud_storage import convert_pdfs_to_pngs as _convert_pdfs_to_pngs


class Main(object):
    def convert_pdfs_to_pngs(
        self,
        source_bucket_name,
        source_path,
        destination_bucket_name=None,
        destination_path=None,
        zpad: int = 4,
    ):
        if not destination_bucket_name:
            destination_bucket_name=source_bucket_name
        if not destination_path:
            destination_path=source_path + "pngs/"
        _convert_pdfs_to_pngs(
            s_bkt=source_bucket_name,
            s_path=source_path,
            d_bkt=destination_bucket_name,
            d_path=destination_path,
            zpad=zpad,
        )


if __name__ == "__main__":
    Fire(Main)
