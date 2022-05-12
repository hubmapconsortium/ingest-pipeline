#!/usr/bin/env python3

import gzip
import random
import sys

from argparse import ArgumentParser
from typing import TextIO


def _open_stream(filename: str, for_writing: bool) -> TextIO:
    # Text mode must be specified explicitly when gzip.open() is used. (It's
    # the default for the builtin open().
    open_mode = 'wt' if for_writing else 'rt'

    if not filename:
        assert for_writing, 'Only the output filename may be omitted.'
        return sys.stdout

    return (
        gzip.open(filename, open_mode) if filename.endswith('.gz')
        else open(filename, open_mode)
    )


def _get_substitute_character(input_char: str, preserve_specials: bool) -> str:
    """This selects a random character, matching the input type.

    * Letters get replaced with A;
    * Digits get replaced with 0;
    * Some instances of @ and + are preserved (when needed for syntax); and
    * all other characters are replaced with *.
    """

    # Note that given quality may contain a random ordering of symbols which
    # may include '@' and '+', here, they must be stripped away as any
    # remaining symbols stored in fixed positions in a string may be
    # identifiable.
    return (
        'A' if input_char.isalpha()
        else '0' if input_char.isdigit()
        else input_char if preserve_specials and input_char in ['@', '+']
        else '*'
    )


def _sterilize_line(line: str, sequence_index: int) -> str:
    assert 1 <= sequence_index <= 4
    return ''.join(
        [_get_substitute_character(c, sequence_index in [1, 3]) for c in line]
    )


def sterilize(input_stream: TextIO, output_stream: TextIO,
              retain_percent: float) -> None:
    output_this_block = False

    for line_number, line in enumerate(input_stream):
        if line_number % 4 == 0:
            output_this_block = random.random() * 100.0 < retain_percent

        if output_this_block:
            output = _sterilize_line(line.strip(), line_number % 4 + 1)
            output_stream.write(f'{output}\n')


def main():
    """Sterilize a given FASTQ file by altering and removing content
    randomly (but consistently), but in a way that retains the proper
    format.
    """
    parser = ArgumentParser(description="""
            Sterilize fastq data to create a variant suitable for testing
            """)
    parser.add_argument('input', metavar='input-filename', type=str,
                        help='Name of the input filename (may be text or .gz)')
    parser.add_argument('output', metavar='output-filename', type=str,
                        nargs='?', default=None,
                        help='Optional name of the output file (may be text '
                             'or .gz), default is text to stdout')
    parser.add_argument('-r', '--retain', metavar='percent', type=float,
                        default=1.0,
                        help='Percentage of records to retain from input')

    args = parser.parse_args()

    with _open_stream(args.input, False) as input_stream, \
            _open_stream(args.output, True) as output_stream:
        sterilize(input_stream, output_stream, args.retain)


if __name__ == '__main__':
    main()
