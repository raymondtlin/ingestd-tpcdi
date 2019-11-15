from __future__ import absolute_import

import click
from ingestd.kafka.clients import ( producers, consumers, operators)

# cli.py
import click

@click.command()
@click.argument('name')
@click.option('--greeting', '-g')
def main(name, greeting):
    click.echo("{}, {}".format(greeting, name))

if __name__ == "__main__":
    main()

