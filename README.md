# versioning-benchmarks

Here we will run benchmarks for different data-versioning tools.

## Setup

### S3 setup

> TODO

### Repositories

Create a XetHub repository and three github repositories for DVC, git LFS and git-annex with readme's.
Here I named them `xethub-py`, `xethub-git`, `versioning-dvc`, `versioning-lfs`, `versioning-lfs-github`,

We clone them locally and setup the remotes:
Setup your git user name and python environment:

```bash
GITUSER=$(git config --global user.name) # or manually set your GitHub/XetHub user name

python -m venv .venv \
&& source .venv/bin/activate \
&& pip install -r requirements.txt

# Download data - takes time! 
python src/download.py --dir=data --download=all --limit=2

# For quick testing
python src/generate.py --dir=mock --count=5 --rows=1000
```

### XetHub setup

1. `git xet clone https://xethub.com/$GITUSER/xethub-py.git xethub-py` # use your own repository
2. `git xet clone https://xethub.com/xdssio/xethub-git.git xethub-git`
2. [Get token](https://xethub.com/user/settings/pat) and setup as environment variables:
    ```bash
      export XET_USER_NAME=<user-name>
      export XET_USER_TOKEN=<xethub-token>
    ```

3. `pip install pyxet`

4. (Optional) [Install CLI](https://xethub.com/assets/docs/getting-started/installation)

#### DVC setup

1. `git clone https://github.com/$GITUSER/versioning-dvc dvc`
2. [Install CLI](https://dvc.org/doc/install)
3. `pip install dvc dvc-s3`
4. setup remote:
    ```bash
   cd dvc
   dvc init
   dvc remote add -d versioning-article s3://<your-bucket-name>/dvc
   dvc remote modify versioning-article region us-west-2
    ```

#### LFS - natural setup

> **Warning:** THIS WILL COST YOU MONEY!
> Limitations:

* GitHub Free and GitHub Pro have a maximum file size limit of 2 GB
* GitHub Team has a maximum file size limit of 4 GB
* GitHub Enterprise Cloud has a maximum file size limit of 5 GB
* Bitbucket Cloud has a maximum file upload limit of 10 GB

Setup:

1. `git clone https://github.com/xdssio/versioning-lfs-github.git lfs-github`
2. [Install CLI](https://github.com/git-lfs/git-lfs?utm_source=gitlfs_site&utm_medium=installation_link&utm_campaign=gitlfs#installing)
3. `cd lfs-github`
4. `git lfs install`
5. `git lfs track '*.parquet'`
6. `git lfs track '*.csv'`
7. `git lfs track '*.txt'`
8. `git add .gitattributes && git commit -m "Enable LFS" && git push`

#### LFS setup + S3

1. `git clone https://github.com/$GITUSER/versioning-lfs lfs-s3`
2. [Install CLI](https://github.com/git-lfs/git-lfs?utm_source=gitlfs_site&utm_medium=installation_link&utm_campaign=gitlfs#installing)
3. `cd lfs-s3`
4. `git lfs install`
5. `git lfs track '*.parquet'`
6. `git lfs track '*.csv'`
7. `git add .gitattributes && git commit -m "Enable LFS" && git push`
8. `cd ..` # so we can setup the server
9. LFS server setup - [Reference](https://github.com/jasonwhite/rudolfs)
    * Generating a random key is easy: `openssl rand -hex 32`
        * Keep this secret and save it in a password manager so you don't lose it. We will pass this to the server
          below.
    * Create a *lfs-server/.env* file with the following contents:
        ```bash
        AWS_ACCESS_KEY_ID=XXXXXXXXXXXXXXXXXXXX
        AWS_SECRET_ACCESS_KEY=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
        AWS_DEFAULT_REGION=us-west-2
        LFS_ENCRYPTION_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx # the result of the openssl command above
        LFS_S3_BUCKET=my-bucket
        LFS_MAX_CACHE_SIZE=10GB
        ```

    * Improve performance (optional)
       ```bash
       # Increase the number of worker threads
       git config --global lfs.concurrenttransfers 64
       # Use a global LFS cache to make re-cloning faster
       git config --global lfs.storage ~/.cache/lfs      
       ```
10. Update the *lfs-s3/.lfsconfig* file:
   ```bash
   [lfs]
   url = "http://http://0.0.0.0:8081/api/my-org/my-project"
              ─────────┬──────── ──┬─ ─┬─ ───┬── ─────┬────
                       │           │   │     │        └ Replace with your project's name
                       │           │   │     └ Replace with your organization name   
                       │           │   └ Required to be "api"
                       │           └ The port your server started with
                       └ The host name of your server
   ```
11. Run local : `docker-compose up`

### LakeFS

1. [Install CLI](https://docs.lakefs.io/reference/cli.html)

* On mac: `brew tap treeverse/lakefs && brew install lakefs`

2. Run using docker metadata and credentials
   ```bash
   mkdir ~/lakefs/metadata  # for persistency 
   docker run --pull always -p 8000:8000 -e LAKEFS_BLOCKSTORE_TYPE='s3' -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -e LAKEFS_DATABASE_LOCAL_PATH=/etc/lakefs/metadata -v ~/lakefs/metadata:/etc/lakefs/metadata treeverse/lakefs run --local-settings
   ```
3. Copy credentials ands save to `~/.lakefs.yaml`.
4. Create a repository and connect to S3 in the UI

# Run

## Prepare docker servers

```bash
# Terminal 1
(cd lfs-server && docker-compose up)
# Terminal 2
docker run --pull always -p 8000:8000 -e LAKEFS_BLOCKSTORE_TYPE='s3' -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -e LAKEFS_DATABASE_LOCAL_PATH=/etc/lakefs/metadata -v ~/lakefs/metadata:/etc/lakefs/metadata treeverse/lakefs run --local-settings
 
# for debug add: 
XET_LOG_LEVEL=debug XET_LOG_PATH=`pwd`/xethub.log 
```

Pull latest data

```bash
cd lfs-github && git pull && cd .. && \
cd lfs-s3 && git pull && cd .. && \
cd dvc && git pull && cd .. && \
cd xethub-git && git pull && cd .. 

```

## Workflows

```python main.py --help```

### test

This upload a small file to all techs

```bash
python main.py test
```

### Peek on latest

This is a quick view of latest results from the terminal.

```bash
python main.py latest

# This let you see the last 10 rows ignoring the experiment id ('track_id').
python main.py latest 10

```

### Append

We simulate a single step with a single technology.    
We generate a new file with a given seed (such as the first rows are always the same for the same seed) - number of rows
is the *start rows* + *add rows X step*.
Params:

```bash
╭─ Options ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --tech                          [s3|pyxet|gitxet|lakefs|lfs-git|lfs-s3|dvc]  The tech to use [default: Tech.pyxet]                                                                                                    │
│ --step                          INTEGER RANGE [x>=0]                         The step to simulate [default: 0]                                                                                                        │
│ --start-rows                    INTEGER                                      How many rows to start with [default: 100000000]                                                                                         │
│ --add-rows                      INTEGER                                      How many rows to add [default: 10000000]                                                                                                 │
│ --suffix                        [csv|parquet|txt]                            What file type to save [default: Suffix.csv]                                                                                             │
│ --diverse       --no-diverse                                                 Whether to generate numeric data [default: no-diverse]                                                                                   │
│ --label                         TEXT                                         The experiment to run [default: default]                                                                                                 │
│ --seed                          INTEGER                                      The seed to use [default: 0]                                                                                                             │
│ --help                                                                       Show this message and exit.                                                                                                              │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
Exmaple:

# This create a csv file with 120 rows - it is equivalent to start with 100 rows, and append 10 rows at step 1 and 2.
python main.py append --tech=pyxet --step=2 --start-rows = 100 --add-rows = 10 --suffix=csv
```

### Benchmark

Execute a benchmark for a given workflow, list of technologies and multiple steps.

```bash
╭─ Arguments ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *    workflow      WORKFLOW:{append|split|taxi}                           The workflow to execute [default: None] [required]                                                                                          │
│      tech          [TECH]:[s3|pyxet|gitxet|lakefs|lfs-git|lfs-s3|dvc]...  The tech to use [default: None (all)]                                                                                                             │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --steps                         INTEGER RANGE [x>=1]  number of steps to run [default: 1]                                                                                                                             │
│ --start-rows                    INTEGER               How many rows to start with [default: 100000000]                                                                                                                │
│ --add-rows                      INTEGER               How many rows to add [default: 10000000]                                                                                                                        │
│ --suffix                        [csv|parquet|txt]     What file type to save [default: Suffix.csv]                                                                                                                    │
│ --diverse       --no-diverse                          If True generate diverse data, default is numeric only [default: no-diverse]                                                                                    │
│ --label                         TEXT                  The experiment to run [default: default]                                                                                                                        │
│ --seed                          INTEGER               The seed to use [default: 0]                                                                                                                                    │
│ --help                                                Show this message and exit.                                                                                                                                     │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

* If only a single step is done, it is equivalent to `random` workflow - as we only generate a single file and upload.
* It is recommended to provide 'label' for each run, so it will be easier to compare results. If not provided and
  steps==1 -> label is random, otherwise it is 'append-{steps}.

