"""
    Example script for using `ginfo` scraper and `SS3` classes
    to manage and store data downloaded from govinfo.gov.
"""


def main():
    import _sys
    from pathlib import Path
    from ginfo.ginfo import Ginfo
    from ginfo.s3 import SS3

    collection = "USCOURTS"
    initial_date = "2000-01-01"
    final_date = "2020-12-12"
    nature_suit = ["Patent"]

    for n in nature_suit:
        g = Ginfo(
            collection=collection,
            nature_suit=n,
            initial_date=initial_date,
            final_date=final_date,
            print_to_console=True,
        )
        # Step 1: Scrape search results and seal initial data with relevant metadata.
        g.seal_results()
        # Step 2: Download pdf and metadata file for each case from the scraped results in Step 1.
        g.parallel_download()
        # Step 3: Serialize relevant (meta)data extracted from Step 3 in bulk into a json file.
        g.bulk_serialize()
        # Step 4: Create a info.json that includes all the information related to the serialized data.
        g.seal_bulk_data()
        # Step 5: Create a gzipped version of the bulk data for storing purposes.
        gzipped_data = g.gzip_bulk_data()
        # Step 6: Create a S3 bucket key to store gzipped_data.
        key = f"{collection}/{n}/{Path(gzipped_data).name}"
        s3 = SS3(secret_key="", public_key="", bucket_name="")
        s3.save(key, gzipped_data)


if __name__ == "__main__":
    main()
