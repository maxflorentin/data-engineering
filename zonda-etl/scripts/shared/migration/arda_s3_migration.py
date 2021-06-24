import subprocess

def get_arda_hdfs_paths(data_date_part):
    output = []
    cmd = 'hdfs dfs -ls -R /santander/business/risk/arda | grep data_date_part={0} | grep -v data_date_part={0}/'.format(data_date_part)

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    out = p.communicate()[0]
    out = out.decode("utf-8")
    for line in out.split('\n'):
        pos = line.strip().find('/santander')
        output.append(line.strip()[pos:len(line.strip())])

    p.wait()
    if p.returncode != 0:
        output = []
        pass

    return output

def send_to_s3(array):
    for path in array:
        cmd = 'hadoop distcp -Dmapred.job.queue.name=root.hdfs-s3 -m 3 -update {0} s3a://sard1ae1ssszonda0plat001{0}'.format(path)
        print (cmd)

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        p.wait()
        if p.returncode != 0:
            pass

def arda_migration(data_date_part):
    array = get_arda_hdfs_paths(data_date_part)
    send_to_s3(array)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Create a ArcHydro schema')

    parser.add_argument('--data_date_part', metavar='data_date_part', required=True,
                        help='data_date_part')

    args = parser.parse_args()

    arda_migration(data_date_part=args.data_date_part)