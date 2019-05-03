import argparse

import layer as l
from utilities import validate as val


def main():
    # Parse commandline arguments
    parser = argparse.ArgumentParser(description='Set input dataset environment.')
    #print 'parser = ' + str(parser)
    parser.add_argument('--dataset', '-d', required=True, help='the tech title of the dataset that has been updated')
    parser.add_argument('--environment', '-e', required=True, choices=('prod', 'staging', 'test'),
                        help='the environment/config files to use')
    print 'parse_args = ' + str(parser)
    #args = parser.parse_args()
    #args = argparse.Namespace(dataset='fires_report', environment='test')
    args = argparse.Namespace(dataset='fires_report', environment='prod')
    #print 'args : ' + str(args)

    print "\n{0}\n{1}\n{0}\n".format('*' * 50, 'GFW Country Pages Analysis v2.0')

    val.validate_inputs(args.dataset)

    # Build layer based on this config table:
    # https://docs.google.com/spreadsheets/d/174wtlPMWENa1FCYXHqzwvZB5vi7DjLwX-oQjaUEdxzo/edit#gid=923735044
    layer = l.Layer(args.dataset, args.environment)

    layer.calculate_summary_values()

    layer.push_to_gfw_api()

    layer.remove_temp_output_dir()


if __name__ == "__main__":
    main()
