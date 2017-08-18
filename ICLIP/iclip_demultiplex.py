#!/usr/bin/env python3
"""Trigger iCount demultiplexing, sample annotation and primary analysis."""

import argparse
import sys

from time import sleep

import resdk
from resdk.resources import Collection


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Annotate and analyse iCount samples.")
    parser.add_argument('multiplexed_reads', help="Multiplexed reads.")
    parser.add_argument('sample_annotation', help="iCount sample annotation file.")
    parser.add_argument('-u', '--username', type=str, help='Username', default='admin')
    parser.add_argument('-p', '--password', type=str, help='Password', default='admin')
    parser.add_argument('-s', '--server', type=str, help='Server URL', default='http://localhost:8000')
    return parser.parse_args()

def get_or_create_collection(resolwe, coll_name):
    """Check if Collection with a given name already exists. Create new Collection if not."""
    n_coll = len(resolwe.collection.filter(name=coll_name))
    if n_coll == 1:
        return resolwe.collection.get(name=coll_name)
    if n_coll == 0:
        new_coll = Collection(resolwe=resolwe)
        new_coll.name = coll_name
        new_coll.save()
        return new_coll

def main():
    """Invoke when run directly as a program."""
    args = parse_arguments()

    # Make a connection to a Resolwe server
    res = resdk.Resolwe(args.username, args.password, args.server)
    resdk.start_logging()

    # Upload input data
    multiplexed_reads = res.run('upload-fastq-single', input={'src': args.multiplexed_reads})
    sample_annotation = res.run('upload-iclip-annotation', input={'src': args.sample_annotation})

    # trigger demultiplexing, annotation and primary analysis
    demultiplex = res.run(
        'workflow-icount-demultiplex',
        input = {
            'reads': multiplexed_reads.id, # reference previously uploaded data by its ID
            'icount_annotation': sample_annotation.id # reference previously uploaded data by its ID
        }
    )

    while demultiplex.status not in ['OK', 'ER']:
        sleep(5)
        demultiplex.update()

    if demultiplex.status == 'OK':

        demux_object = res.data.filter(parents=demultiplex.id, type='data:demultiplex:icount:')[0]
        annotation_object = res.data.filter(parents=demultiplex.id, type='data:icount:annotsample:')[0]

        # wait for sample annotation job to either finish or fail
        while annotation_object.status not in ['OK', 'ER']:
            sleep(5)
            annotation_object.update()

        if annotation_object.status == 'OK':
            # get a list of child objects after demultiplexing
            demux_data = res.data.filter(parents=demux_object.id)

            # Use experiment_name annotation field to assign samples to collection
            for d_obj in demux_data:
                if d_obj.sample:
                    try:
                        collection_name = d_obj.descriptor['other']['experiment_name']
                        coll = get_or_create_collection(res, collection_name)

                        if coll:
                            coll.add_samples(d_obj.sample)
                            print('{} added to collection {}.'.format(d_obj.sample, coll.name))
                    except:
                        print('{} was not assigned to any collections'.format(d_obj.sample))
        else:
            print('Sample annotation failed. Could not assign samples to collection.')
    else:
        print('Demultiplexing job failed')
        sys.exit(1)

if __name__ == "__main__":
    main()
