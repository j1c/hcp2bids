import json
from pathlib import Path

import bids
from bids import BIDSLayout

bids.config.set_option("extension_initial_dot", True)

pattern = (
    "sub-{subject}/{datatype<anat|dwi>}/sub-{subject}"
    "[_ses-{session}][_acq-{acquisition}][_dir-{direction}][_run-{run}]"
    "_{suffix<T[12]w|dwi>}{extension<.bval|.bvec|.json|.nii.gz>|.nii.gz}"
)
dir_pattern = "sub-{subject}[/ses-{session}]/{datatype<anat|dwi>}/"
modality_dict = dict(Diffusion="dwi", T1w_MPR1="anat")
run_dict = dict(dir95="1", dir96="2", dir97="3")


def _mkdir(layout, subject, modality):
    entities = dict(subject=subject, datatype=modality_dict[modality])
    dir_name = Path(layout.build_path(entities, dir_pattern, validate=False))
    dir_name.mkdir(parents=True, exist_ok=True)


def convert(input_path, output_path):
    in_path = Path(input_path)
    if not in_path.is_dir():
        msg = f"{input_path} is not a valid directory."
        raise ValueError(msg)

    # Make output path
    out_path = Path(output_path)
    if not out_path.is_dir():
        out_path.mkdir(parents=True)

    # Generate dataset_description.json
    data = dict(Name="hcp", BIDSVersion="1.4.0", DatasetType="raw")
    with open(out_path / "dataset_description.json", "w") as f:
        json.dump(data, f)

    layout = BIDSLayout(out_path.absolute())

    # Iterate through each subject folder
    subject_folders = [x for x in in_path.iterdir() if x.is_dir()]
    for subject_folder in subject_folders:
        if not (subject_folder / "unprocessed/3T/").is_dir():
            continue

        # 6 digit sub id in str form
        subject = subject_folder.name

        modality_folders = [
            x for x in (subject_folder / "unprocessed/3T/").iterdir() if x.is_dir()
        ]
        for modality_folder in modality_folders:
            modality = modality_folder.name

            # Make bids output folders
            _mkdir(layout, subject, modality)

            if modality == "T1w_MPR1":
                entities = dict(
                    subject=subject,
                    datatype=modality_dict[modality],
                    extension=".nii.gz",
                    suffix="T1w",
                )
                new_fname = layout.build_path(entities, pattern)

                # Rename old files
                old_fname = list(modality_folder.iterdir())[0]
                old_fname.rename(new_fname)

            elif modality == "Diffusion":
                for fname in modality_folder.iterdir():
                    splits = fname.name.split(".")
                    extension = "." + splits[-1]  # Get extension
                    if extension == ".gz":
                        extension = ".nii.gz"
                    splits = splits[0].split("_")
                    direction = splits[-1]  # Direction. RL or LR
                    run = run_dict[splits[-2]]  # Run number

                    entities = dict(
                        subject=subject,
                        datatype=modality_dict[modality],
                        direction=direction,
                        run=run,
                        extension=extension,
                        suffix="dwi",
                    )
                    new_fname = layout.build_path(entities, pattern)
                    Path(fname).rename(new_fname)

                    # Make json sidecar
                    if extension == ".nii.gz":
                        entities["extension"] = ".json"

                        if direction == "LR":
                            phase = "i-"
                        elif direction == "RL":
                            phase = "i"

                        # TotalReadoutTime = EffectiveEchoSpacing * (EPI factor - 1) (which is 144)
                        sidecar = dict(
                            EffectiveEchoSpacing=0.00078,
                            TotalReadoutTime=0.11154,
                            PhaseEncodingDirection=phase,
                        )
                        with open(layout.build_path(entities, pattern), "w") as f:
                            json.dump(sidecar, f)

            # Remove all folders
            modality_folder.rmdir()

        for folder in list(subject_folder.rglob("*"))[::-1]:
            folder.rmdir()
        subject_folder.rmdir()

    if not input_path == output_path:
        in_path.rmdir()
