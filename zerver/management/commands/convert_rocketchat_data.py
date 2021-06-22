import argparse
import os
import tempfile
from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser

from zerver.data_import.rocketchat1 import do_convert_data

fallback_avatar_url = ('https://user-images.githubusercontent.com/35293767/'
                       '34953768-88f630a0-fa26-11e7-9589-020d002fcc5b.png')


class Command(BaseCommand):
    help = """Convert the Rocketchat data into Zulip data format."""

    def add_arguments(self, parser: CommandParser) -> None:
        dir_help = (
            "Directory containing all the `bson` files from mongodb dump of rocketchat."
        )
        parser.add_argument(
            "rocketchat_data_dir", metavar="<rocketchat data directory>", help=dir_help
        )

        parser.add_argument(
            "--output", dest="output_dir", help="Directory to write converted data to."
        )
        paa = parser.add_argument
        # paa('--output', dest='output_dir',
        #     action="store", default=None,
        #     help='Directory to write exported data to.')
        # paa('-R', '--rocketchat_dump', help="Dir with Rocketchat MongoDB dump")
        paa('-A', '--fallback_avatar', default=fallback_avatar_url,
            help="Provide URL to custom avatar. Default: {}".format(
                fallback_avatar_url))
        paa('-l', '--loglevel', help="Set log level", default="info")

        parser.formatter_class = argparse.RawTextHelpFormatter

    def handle(self, *args: Any, **options: Any) -> None:
        output_dir = options["output_dir"]
        if output_dir is None:
            raise CommandError("You need to specify --output <output directory>")
            # output_dir = tempfile.mkdtemp(prefix="converted-rocketchat-data-")

        if os.path.exists(output_dir) and not os.path.isdir(output_dir):
            raise CommandError(output_dir + " is not a directory")

        os.makedirs(output_dir, exist_ok=True)

        if os.listdir(output_dir):
            raise CommandError("Output directory should be empty!")
        output_dir = os.path.realpath(output_dir)

        data_dir = options["rocketchat_data_dir"]
        if not os.path.exists(data_dir):
            raise CommandError(f"Directory not found: '{data_dir}'")
        data_dir = os.path.realpath(data_dir)

        print("Converting Data ...")
        # do_convert_data(output_dir, options)
        do_convert_data(rocketchat_data_dir=data_dir, output_dir=output_dir)
