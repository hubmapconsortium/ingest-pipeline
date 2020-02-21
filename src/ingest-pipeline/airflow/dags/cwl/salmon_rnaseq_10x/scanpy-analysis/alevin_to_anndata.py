#!/usr/bin/env python3
"""
Adapted from https://github.com/mruffalo/vpolo/blob/master/vpolo/alevin/parser.py
which is licensed under the GPL v3 (so this file is as well)
"""
import gzip
import sys
from argparse import ArgumentParser
from pathlib import Path
from struct import Struct

import numpy as np
import pandas as pd
import anndata

def convert(quant_file: Path, gene_file: Path, cb_file: Path, density="sparse"):
    """
    Read the quants sparse binary output of Alevin and converts to an `anndata` object

    :param quant_file: quants_mat.gz from Alevin
    :param gene_file: quants_mat_cols.txt from Alevin
    :param cb_file: quants_mat_rows.txt from Alevin
    :param density: one of {'sparse', 'dense'}
    """
    data_type = "f"

    cb_names = pd.read_csv(cb_file, header=None)[0].values
    gene_names = pd.read_csv(gene_file, header=None)[0].values
    num_genes = len(gene_names)
    num_entries = int(np.ceil(num_genes/8))

    with gzip.open(quant_file) as f:
        line_count = 0
        tot_umi_count = 0
        umi_matrix = []

        if density == "sparse":
            header_struct = Struct("B" * num_entries)
            while True:
                line_count += 1
                if not (line_count % 100):
                    print("\rDone reading", line_count, "cells", end="")
                    sys.stdout.flush()
                try:
                    num_exp_genes = 0
                    exp_counts = header_struct.unpack_from(f.read(header_struct.size))
                    for exp_count in exp_counts:
                        num_exp_genes += bin(exp_count).count("1")

                    data_struct = Struct(data_type * num_exp_genes)
                    sparse_cell_counts_vec = list(data_struct.unpack_from(f.read(data_struct.size)))[::-1]
                    cell_umi_counts = sum(sparse_cell_counts_vec)

                except Exception:
                    print("\nRead total", line_count - 1, " cells")
                    print("Found total", tot_umi_count, "reads")
                    break

                if cell_umi_counts > 0.0:
                    tot_umi_count += cell_umi_counts

                    cell_counts_vec = []
                    for exp_count in exp_counts:
                        for bit in format(exp_count, '08b'):
                            if len(cell_counts_vec) >= num_genes:
                                break

                            if bit == '0':
                                cell_counts_vec.append(0.0)
                            else:
                                abund = sparse_cell_counts_vec.pop()
                                cell_counts_vec.append(abund)

                    if len(sparse_cell_counts_vec) > 0:
                        print("Failure in consumption of data")
                        print("left with {} entry(ies)".format(len(sparse_cell_counts_vec)))
                    umi_matrix.append(cell_counts_vec)
                else:
                    raise ValueError("Found a CB with no read count, something is wrong")
        elif density == "dense":
            header_struct = Struct("d" * num_genes)
            while True:
                line_count += 1
                if not (line_count % 100):
                    print("\rDone reading", line_count, "cells", end="")
                    sys.stdout.flush()

                try:
                    cell_counts = header_struct.unpack_from(f.read(header_struct.size))
                except Exception:
                    print("\nRead total", line_count - 1, " cells")
                    print("Found total", tot_umi_count, "reads")
                    break

                read_count = 0.0
                for x in cell_counts:
                    read_count += float(x)
                tot_umi_count += read_count

                if read_count > 0.0:
                    umi_matrix.append(cell_counts)
                else:
                    raise ValueError('Found a CB with no read count, something is wrong')
        else:
            raise ValueError(f'Wrong density parameter: {density}')

    alv = pd.DataFrame(umi_matrix, columns=gene_names, index=cb_names)

    return anndata.AnnData(alv)

if __name__ == '__main__':
    p = ArgumentParser()
    p.add_argument('quants_mat_file', type=Path)
    p.add_argument('quants_mat_cols', type=Path)
    p.add_argument('quants_mat_rows', type=Path)
    args = p.parse_args()

    alv = convert(args.quants_mat_file, args.quants_mat_cols, args.quants_mat_rows)
    alv.write_h5ad('out.h5ad')
