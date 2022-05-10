#!/usr/bin/env python3

import gzip
import random
import string
import sys

from argparse import ArgumentParser
from typing import TextIO

ALPHABETIC_CHARS = string.ascii_uppercase
NUMERIC_CHARS = string.digits
RNA_CHARS = 'ACGT'
QUALITY_CHARS = string.digits + string.ascii_letters + string.punctuation


def _get_random_alpha() -> str:
    return random.choice(ALPHABETIC_CHARS)


def _get_random_digit() -> str:
    return random.choice(NUMERIC_CHARS)


def _get_random_rna() -> str:
    return random.choice(RNA_CHARS)


def _get_random_quality() -> str:
    return random.choice(QUALITY_CHARS)


def _get_random_matching_character(input_char) -> str:
    """This selects a random character, matching the input type.

    * Letters get replaced with random, uppercase letters;
    * Digits get replaced with random digits; and
    * all other characters are copied (they may be significant).
    """
    return (
        _get_random_alpha() if str.isalpha(input_char)
        else _get_random_digit() if str.isdigit(input_char)
        else input_char
    )


def _open_stream(filename: str, for_writing: bool) -> TextIO:
    open_mode = 'w' if for_writing else 'r'

    if not filename:
        assert for_writing, 'Only the output filename may be omitted.'
        return sys.stdout

    return (
        gzip.open(filename, open_mode) if filename.endswith('.gz')
        else open(filename, open_mode)
    )


def _sterilize_line1_or_3(line: str) -> str:
    """Line 1 begins with a '@' character and is followed by a sequence
    identifier and an optional description (like a FASTA title line).
    Line 3 begins with a '+' character and is optionally followed by the
    same sequence identifier (and any description) again.
    """
    output = str([_get_random_matching_character(c) for c in line])

    return output


def _sterilize_line2(line: str) -> str:
    """Line 2 is the raw sequence letters."""
    output = ''
    for _ in line:
        output += _get_random_rna()

    return output


def _sterilize_line4(line: str) -> str:
    """Line 4 encodes the quality values for the sequence in Line 2, and
    must contain the same number of symbols as letters in the sequence.
    """
    output = ''
    for _ in line:
        output += _get_random_quality()

    return output


def _sterilize_line(line: str, sequence_index: int) -> str:
    assert 1 <= sequence_index <= 4

    return (
        _sterilize_line2(line) if sequence_index == 2
        else _sterilize_line4(line) if sequence_index == 4
        else _sterilize_line1_or_3(line)
    )


def sterilize(input_stream: TextIO, output_stream: TextIO,
              retain_percent: float) -> None:
    output_this_block = False

    for sequence_index, line in enumerate(input_stream):
        if sequence_index % 4 == 0:
            output_this_block = random.random() * 100.0 < retain_percent

        if output_this_block:
            output = _sterilize_line(line.strip(), sequence_index % 4 + 1)
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
