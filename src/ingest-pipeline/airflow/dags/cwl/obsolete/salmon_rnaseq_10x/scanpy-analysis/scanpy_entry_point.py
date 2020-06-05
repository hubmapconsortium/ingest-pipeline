#!/usr/bin/env python3
from argparse import ArgumentParser
from contextlib import contextmanager
from pathlib import Path

import anndata
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scanpy as sc

@contextmanager
def new_plot():
    """
    When used in a `with` block, clears matplotlib internal state
    after plotting and saving things. Probably not necessary to be this
    thorough in clearing everything, but extra calls to `plt.clf()` and
    `plf.close()` don't *hurt*

    Intended usage:
        ```
        with new_plot():
            do_matplotlib_things()

            plt.savefig(path)
            # or
            fig.savefig(path)
        ```
    """
    plt.clf()
    try:
        yield
    finally:
        plt.clf()
        plt.close()

def qc_checks(args):
    adata = anndata.read_h5ad(args.filtered_feature_matrix)
    adata.var_names_make_unique()

    qc_by_cell, qc_by_gene = sc.pp.calculate_qc_metrics(adata)

    # current directory is set up by the CWL runner
    qc_path = Path('qc_results.hdf5').absolute()
    print('Saving QC results to', qc_path)
    with pd.HDFStore(qc_path) as store:
        store['qc_by_cell'] = qc_by_cell
        store['qc_by_gene'] = qc_by_gene

def filter_normalize(args):
    adata = anndata.read_h5ad(args.filtered_feature_matrix)
    adata.var_names_make_unique()

    sc.pp.filter_cells(adata, min_genes=200)
    sc.pp.filter_genes(adata, min_cells=3)

    # add the total counts per cell as observations-annotation to adata
    adata.obs['n_counts'] = adata.X.sum(axis=1)

    adata.raw = sc.pp.log1p(adata, copy=True)

    sc.pp.normalize_per_cell(adata, counts_per_cell_after=1e4)

    filter_result = sc.pp.filter_genes_dispersion(adata.X, min_mean=0.0125, max_mean=5, min_disp=0)
    sc.pl.filter_genes_dispersion(filter_result, show=False, save='dispersion.pdf')

    # %%
    adata = adata[:, filter_result.gene_subset]
    sc.pp.scale(adata, max_value=10)

    filtered_path = Path('filtered_normalized.h5ad').absolute()
    print('Saving filtered normalized expression data to', filtered_path)
    adata.write_h5ad(filtered_path)

def dim_reduce_cluster(args):
    filtered_normalized = anndata.read_h5ad(args.filtered)

    sc.pp.neighbors(filtered_normalized, n_neighbors=10, n_pcs=10)
    sc.tl.umap(filtered_normalized)

    with new_plot():
        sc.pl.umap(filtered_normalized)
        plt.savefig('umap.pdf', bbox_inches='tight')

    sc.tl.leiden(filtered_normalized)

    with new_plot():
        sc.pl.umap(filtered_normalized, color='leiden')
        plt.savefig('umap_by_leiden_cluster.pdf', bbox_inches='tight')

    filtered_normalized.write_h5ad('dim_reduced_clustered.h5ad')

def compute_cluster_marker_genes(args):
    dim_reduced_clustered = anndata.read_h5ad(args.clusters)

    sc.tl.rank_genes_groups(dim_reduced_clustered, 'leiden', method='t-test')

    with new_plot():
        sc.pl.rank_genes_groups(dim_reduced_clustered, n_genes=25, sharey=False)
        plt.savefig('marker_genes_by_cluster_t_test.pdf', bbox_inches='tight')

    sc.tl.rank_genes_groups(dim_reduced_clustered, 'leiden', method='logreg')

    with new_plot():
        sc.pl.rank_genes_groups(dim_reduced_clustered, n_genes=25, sharey=False)
        plt.savefig('marker_genes_by_cluster_logreg.pdf', bbox_inches='tight')

    # Save normalized/etc. data
    dim_reduced_clustered.write_h5ad('cluster_marker_genes.h5ad')

if __name__ == '__main__':
    p = ArgumentParser()
    subparsers = p.add_subparsers()

    parser_qc_checks = subparsers.add_parser('qc_checks')
    parser_qc_checks.add_argument('filtered_feature_matrix', type=Path)
    parser_qc_checks.set_defaults(func=qc_checks)

    parser_filter_normalize = subparsers.add_parser('filter_normalize')
    parser_filter_normalize.add_argument('filtered_feature_matrix', type=Path)
    parser_filter_normalize.set_defaults(func=filter_normalize)

    parser_dim_reduce_cluster = subparsers.add_parser('dim_reduce_cluster')
    parser_dim_reduce_cluster.add_argument('filtered', type=Path)
    parser_dim_reduce_cluster.set_defaults(func=dim_reduce_cluster)

    parser_compute_cluster_marker_genes = subparsers.add_parser('compute_cluster_marker_genes')
    parser_compute_cluster_marker_genes.add_argument('clusters', type=Path)
    parser_compute_cluster_marker_genes.set_defaults(func=compute_cluster_marker_genes)

    args = p.parse_args()
    args.func(args)
