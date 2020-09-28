import os
from pathlib import Path

import boto3


def get_all_subjects(bucket, prefix, client):
    def get_subjects():
        continuation_token = None

        while True:
            list_kwargs = dict(Bucket=bucket, Prefix=prefix, Delimiter="/")
            if continuation_token:
                list_kwargs["ContinuationToken"] = continuation_token
            response = client.list_objects_v2(**list_kwargs)
            yield from response.get("CommonPrefixes", [])
            if not response.get("IsTruncated"):  # At the end of the list?
                break
            continuation_token = response.get("NextContinuationToken")

    # Returns all the 6-digit subject codes in a given bucket
    subject_list = [d["Prefix"].split("/")[1] for d in get_subjects()]

    return subject_list


def get_data(
    output_path,
    subjects=None,
    prefix="HCP_1200/",
    verbose=False,
    access_key_id=None,
    secret_access_key=None,
    profile_name=None,
):
    """
    Do not hard code access key and secret key.

    Parameters
    ----------
    access_key_id : str

    secret_access_key : str

    output_path : str, list
        Path to the output files

    subject : str, optional (default=None)
        If None, download all available subjects.

    prefix : str
        One of {"HCP_1200/", "HCP_Retest/"}

    verbose : bool, default=False
    """
    if profile_name is not None:
        s3 = boto3.client("s3", profile_name=profile_name)
    elif (access_key_id is not None) & (secret_access_key is not None):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )
    else:
        msg = f"Profile name or access keys must be provided."
        raise ValueError(msg)

    bucket = "hcp-openaccess"

    # get all subjects
    all_subjects = get_all_subjects(bucket, prefix, s3)

    if subjects is not None:
        # Check all input subjects are valid
        if isinstance(subjects, str):
            if subjects not in all_subjects:
                msg = f"{subjects} is not a valid subject number."
                raise ValueError(msg)
            subjects = [subjects]
        elif isinstance(subjects, list):
            invalid_subjects = []
            for subject in subjects:
                if subject not in all_subjects:
                    invalid_subjects.append(subject)
            if len(invalid_subjects) != 0:
                msg = f"{', '.join(invalid_subjects)} are not valid subject number(s)."
                raise ValueError(msg)
        else:
            msg = f"subjects must be a string or list."
            raise TypeError(msg)
    else:
        subjects = all_subjects

    # Make output directories
    p = Path(output_path)

    # Iteratively download each subject
    for subject in subjects:
        if verbose:
            print(f"Downloading Subject: {subject}...")

        to_download = []

        # Diffusion
        contents = s3.list_objects(
            Bucket="hcp-openaccess",
            Prefix=f"{prefix}{subject}/unprocessed/3T/Diffusion/",
        ).get("Contents")

        for c in contents:
            fname = c.get("Key")
            if not any(
                exclude in fname for exclude in ["BIAS", "SBRef", "LINKED_DATA"]
            ):
                to_download.append(fname)

        # T1w
        contents = s3.list_objects(
            Bucket="hcp-openaccess",
            Prefix=f"{prefix}{subject}/unprocessed/3T/T1w_MPR1/",
        ).get("Contents")

        for c in contents:
            fname = c.get("Key")
            if fname.endswith("T1w_MPR1.nii.gz"):
                to_download.append(fname)

        for key in to_download:
            filename = p / key.replace(prefix, "")
            if not filename.parents[0].exists():
                filename.parents[0].mkdir(parents=True)

            if verbose:
                print(f"Downloading File: {filename}...")

            if filename.exists():
                if verbose:
                    print(f"Skipping File: {filename}. Already exists...")
            else:
                s3.download_file(Bucket=bucket, Key=key, Filename=str(filename))

        if verbose:
            print(f"Finished Downloading Subject: {subject}...")