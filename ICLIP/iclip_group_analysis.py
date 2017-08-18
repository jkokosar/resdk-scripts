#!/usr/bin/env python3
"""Run group analysis on samples contained withing a collection."""

import argparse
import sys

import resdk


SEGMENTATION_MAP = {
    'Homo sapiens': 'icount-segmentation-hs',
    'Mus musculus': 'icount-segmentation-mm'
}

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run group analysis on samples contained withing a collection.")
    parser.add_argument('-i', '--id', type=int, nargs='+', help="Sample IDs to be grouped.")
    parser.add_argument('-c', '--collection', type=str, required=True, help="Working collection (name).")
    parser.add_argument('-n', '--name', type=str, required=True, help="Group name.")
    parser.add_argument('-u', '--username', type=str, help='Username', default='admin')
    parser.add_argument('-p', '--password', type=str, help='Password', default='admin')
    parser.add_argument('-s', '--server', type=str, help='Server URL', default='http://localhost:8000')
    return parser.parse_args()

def get_collection(resolwe, coll_name):
    """Check if a single Collection with a given name exists."""
    try:
        return resolwe.collection.get(name=coll_name)
    except:
        print('Could not fetch a collection {} '
              'Collection either does not exists or multiple collections '
              'with the same name were found'.format(coll_name))
        sys.exit(1)

def get_xlsites(sample):
    """Return ``xlsites`` object on the sample."""
    return sample.data.get(type='data:bed:icount:')

def get_species(sample):
    """Return value of the organism field from sample descriptor."""
    return sample.descriptor['sample']['organism']


def main():
    """Invoke when run directly as a program."""
    args = parse_arguments()

    # Make a connection to a Resolwe server
    res = resdk.Resolwe(args.username, args.password, args.server)
    resdk.start_logging()

    # Set working collection
    collection = get_collection(res, args.collection)

    # if a list of sample ids is provided, use those samples for iCount group analysis
    # else, group all the samples contained in a working collection
    if args.id:
        xlsites = [get_xlsites(res.sample.get(sample_id)).id for sample_id in args.id]
    else:
        xlsites = [get_xlsites(sample).id for sample in collection.samples]

    # Check sample species
    if args.id:
        species = set([get_species(res.sample.get(sample_id)) for sample_id in args.id])
    else:
        species = set([get_species(sample) for sample in collection.samples])

    if len(species) != 1:
        print('Selected samples belong to multiple species, cannot run group analysis.')
        sys.exit(1)

    # Run iCount secondary analysis on grouped samples
    group_analysis = res.run(
        'workflow-icount-group-analysis',
        collections = [collection.id],
        input = {
            'sites': xlsites,
            'group_name': args.name,
            'segmentation': res.data.get(SEGMENTATION_MAP[species.pop()]).id
        }
    )

if __name__ == "__main__":
    main()
