"""A parser of Job Description File. """

import argparse
import os

from . import job


def parse_jobfile(jobfile_name):

    job_description_dict = {
        'name': 'defaultJob',
        'executable': None,
        'priority': 0,
        'time_required': None,
        'min_memory': None,
        'min_cores': None,
        'max_memory': None,
    }

    with open(jobfile_name, "r") as jobfile:
        for line in jobfile:
            field, value = [val.strip() for val in line.strip().split(':')]
            if field not in job_description_dict:
                raise ValueError("Invalid job description file: Field %s not "
                                 "recognized" % field)
            else:
                job_description_dict[field] = value

    # Check that values for all attributes have been received
    for value in job_description_dict.values():
        if value is None:
            raise ValueError(
                "Invalid job description file: Missing fields found")

    # Check that executable exists

    return job_description_dict


def make_job(jobfile_name):
    job_description_dict = parse_jobfile(jobfile_name)
    created_job = job.Job(**job_description_dict)
    return created_job


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-filename", help="job descriptor file name", type=str)
    args = vars(parser.parse_args())
    job_filename = args['filename']
    make_job(job_filename)
    print('Job made!')