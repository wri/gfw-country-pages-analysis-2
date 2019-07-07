import argparse

from gfw_country_pages_analysis_2 import layer as ly
from gfw_country_pages_analysis_2.utilities import validate as val, log


def main():

    try:
        # Parse commandline arguments
        parser = argparse.ArgumentParser(description="Set input dataset environment.")
        parser.add_argument(
            "--dataset",
            "-d",
            required=True,
            help="the tech title of the dataset that has been updated",
        )
        parser.add_argument(
            "--environment",
            "-e",
            required=True,
            choices=("prod", "staging", "test"),
            help="the environment/config files to use",
        )
        log.debug("parse_args = " + str(parser))
        args = parser.parse_args()

        log.info("Start updating {} country stats".format(args.dataset))
        log.info(
            "\n{0}\n{1}\n{0}\n".format("*" * 50, "GFW Country Pages Analysis v2.0")
        )

        val.validate_inputs(args.dataset)

        # Build layer based on this config table:
        # https://docs.google.com/spreadsheets/d/174wtlPMWENa1FCYXHqzwvZB5vi7DjLwX-oQjaUEdxzo/edit#gid=923735044
        layer = ly.Layer(args.dataset, args.environment)

        layer.calculate_summary_values()

        layer.push_to_gfw_api()

        layer.remove_temp_output_dir()
        log.info("Successfully updated {} country stats.".format(args.dataset))
    except Exception as e:
        log.error(
            "Failed to update {} country stats. Please see log files for more info".format(
                args.dataset
            ),
            True,
        )
        log.error(e)
        raise e


if __name__ == "__main__":
    main()
