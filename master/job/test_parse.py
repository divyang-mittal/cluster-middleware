from . import job_parser

f = job_parser.parse_jobfile('/home/fenris/Documents/8th_semester/DistributedSystems/termProject/FinalOne/cluster-middleware/master/job/sample_jobfile')
print(f)
print("\n\n\n")

f = job_parser.make_job('/home/fenris/Documents/8th_semester/DistributedSystems/termProject/FinalOne/cluster-middleware/master/job/sample_jobfile')
print(f.sender)