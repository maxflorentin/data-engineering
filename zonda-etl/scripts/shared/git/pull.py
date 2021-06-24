import io
import os
import subprocess
import sys

GIT_BRANCH = os.getenv('GIT_BRANCH', 'master')
ZONDA_DIR = os.getenv('ZONDA_DIR')
REPOSITORIES = [ZONDA_DIR + '/repositories/zonda-etl:' + GIT_BRANCH]
INPUT = {"repositories": "List of repositories to pull, separated by comma (absolute paths)"}


def pull(repositories_conf):
    try:
        # repositories to clone
        repositories = []
        tmp = REPOSITORIES
        if repositories_conf and repositories_conf != 'None':
            tmp = list(set(repositories_conf.split(',')))

        for a in tmp:
            repo_splitted = a.split(':')
            path = repo_splitted[0]
            repo = dict()
            repo['name'] = [x for x in path.split(os.sep) if x][-1]
            repo['path'] = path
            repo['branch'] = repo_splitted[1] if len(repo_splitted) == 2 else 'master'
            repositories.append(repo)

        for repo in repositories:
            # change working directory
            print('INFO - Repository [{}]'.format(repo['name']), flush=True)
            print('INFO - Changing working directory...', flush=True)
            os.chdir(repo['path'])

            # switch branch master
            branch = repo['branch']
            print('INFO - Checking out {} branch...'.format(branch), flush=True)
            cmd = ['git', 'checkout', '-f', branch]
            print(' '.join(cmd))
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)
            p.wait()
            if p.returncode != 0:
                raise Exception()

            # pull changes
            print('INFO - Pulling changes from origin [{}]...'.format(branch), flush=True)
            cmd = ['git', 'pull', 'origin', branch]
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)

            for line in io.TextIOWrapper(p.stdout, encoding="utf-8"):
                if any(x in line.upper() for x in ['ERROR', 'CRITICAL']):
                    print('CRIT - {}'.format(line.strip()))
                if any(x in line.upper() for x in ['WARN', 'WARNING']):
                    print('WARN - {}'.format(line.strip()))
                else:
                    print('INFO - {}'.format(line.strip()))

            p.wait()
            if p.returncode != 0:
                raise Exception()
            print('INFO - Done\n', flush=True)
    except Exception as ex:
        print('CRIT - ' + str(ex.args[1]))
        exit(1)


if __name__ == '__main__':
    if len(sys.argv) < 1:
        raise Exception('Missing arguments')

    repositories_conf = sys.argv[1]
    pull(repositories_conf)
