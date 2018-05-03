import google_sheet as gs


def validate_inputs(dataset):

    valid_datasets = gs.get_valid_inputs()

    if dataset not in valid_datasets:
        print valid_datasets
        raise ValueError('Dataset {0} is not listed in the forest change or contextual '
                         'layers Google Doc.'.format(dataset))

