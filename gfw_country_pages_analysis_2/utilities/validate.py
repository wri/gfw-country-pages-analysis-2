import google_sheet as gs
import log


def validate_inputs(dataset):

    valid_datasets = gs.get_valid_inputs()

    if dataset not in valid_datasets:
        log.info("Valid datasets: " + valid_datasets)
        log.error(
            "Dataset {0} is not listed in the forest change or contextual "
            "layers Google Doc.".format(dataset)
        )
        raise ValueError(
            "Dataset {0} is not listed in the forest change or contextual "
            "layers Google Doc.".format(dataset)
        )
