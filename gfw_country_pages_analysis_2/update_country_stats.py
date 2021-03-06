import argparse

from gfw_country_pages_analysis_2.layers import Layer, GladLayer
from gfw_country_pages_analysis_2.utilities import validate as val, log
from gfw_country_pages_analysis_2.factory import LayerFactory


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

        factory = LayerFactory()
        factory.register_layer("umd_landsat_alerts", GladLayer)
        factory.register_layer("fires_report", Layer)
        factory.register_layer("fires_country_pages", Layer)
        factory.register_layer("terra_i_alerts", Layer)

        # Build layer based on this config table:
        # https://docs.google.com/spreadsheets/d/174wtlPMWENa1FCYXHqzwvZB5vi7DjLwX-oQjaUEdxzo/edit#gid=923735044
        layer = factory.get_layer(args.dataset, args.environment)

        try:
            layer.calculate_summary_values()

            layer.push_to_gfw_api()

            layer.remove_temp_output_dir()

            layer.finalize()

            log.info("Successfully updated {} country stats.".format(args.dataset))
        except Exception as e:
            layer.finalize(failed=True)
            raise e
    except Exception as e:
        log.error(
            "Failed to update {} country stats. Please see log files for more info".format(
                args.dataset
            ),
            True,
        )
        log.exception(e)
        raise e


if __name__ == "__main__":
    main()
