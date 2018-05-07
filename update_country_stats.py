import argparse

import layer as l
from utilities import validate as val


def main():
    # -d umd_landsat_alerts_iso -e test
    # Parse commandline arguments
    parser = argparse.ArgumentParser(description='Set input dataset environment.')
    parser.add_argument('--dataset', '-d', required=True, help='the tech title of the dataset that has been updated')
    parser.add_argument('--environment', '-e', required=True, choices=('prod', 'staging', 'test'),
                        help='the environment/config files to use')
    args = parser.parse_args()

    print "\n{0}\n{1}\n{0}\n".format('*' * 50, 'GFW Country Pages Analysis v2.0')

    val.validate_inputs(args.dataset)

    # Build layer based on this config table:
    # https://docs.google.com/spreadsheets/d/174wtlPMWENa1FCYXHqzwvZB5vi7DjLwX-oQjaUEdxzo/edit#gid=923735044
    layer = l.Layer(args.dataset, args.environment)

    layer.calculate_summary_values()

    #layer.push_to_gfw_api()


if __name__ == "__main__":
    main()
