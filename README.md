# versioning-benchmarks

Here we will run benchmarks for different data-versioning tools.

## Setup

### S3 setup

> TODO

### Repositories

Create a XetHub repository and three github repositories for DVC, git LFS and git-annex with readme's.
Here I named them `versioning-xethub`, `versioning-dvc`, `versioning-lfs`.

We clone them locally and setup the remotes:
Setup your git user name and python environment:

```bash
USER=$(git config --global user.name) # or manually set your GitHub/XetHub user name

python -m venv .venv \
&& source .venv/bin/activate \
&& pip install -r requirements.txt

# Download data - takes time! 
python src/download.py --dir=data --download=all --limit=2

# For quick testing
python src/generate.py --dir=mock --count=5 --rows=1000
```

### XetHub setup

1. `git xet clone https://xethub.com/${USER}/versioning-xethub xethub`
2. [Get token](https://xethub.com/user/settings/pat) and setup as environment variables:
    ```bash
      export XET_USER_NAME=<user-name>
      export XET_USER_TOKEN=<xethub-token>
    ```

3. `pip install pyxet`

4. (Optional) [Install CLI](https://xethub.com/assets/docs/getting-started/installation)

#### LFS setup

1. `git clone https://github.com/${USER}/versioning-lfs lfs`
2. [Install CLI](https://github.com/git-lfs/git-lfs?utm_source=gitlfs_site&utm_medium=installation_link&utm_campaign=gitlfs#installing)
3. `cd lfs`
4. `git lfs install`
5. Create a *.lfsconfig* file with:
   ```yaml
    [lfs]
    url = https://<your-bucket-name>.s3.amazonaws.com
    ``` 
7. `git lfs track *.parquet
8. Setup git  config:
   ```yaml
   git config lfs.storage.type "s3"
   git config lfs.storage.s3.bucket "versioning-article"
   git config lfs.storage.s3.region "us-west-2"  # your S3 bucket region
   git config lfs.storage.s3.accesskeyid "YOUR_AWS_ACCESS_KEY_ID"  
   git config lfs.storage.s3.secretaccesskey "YOUR_AWS_SECRET_ACCESS_KEY"


8. git lfs track --external --local --set-upstream s3://bucket_name` ??

#### DVC setup
1. `git clone https://github.com/${USER}/versioning-dvc dvc`
2. [Install CLI](https://dvc.org/doc/install)
3. `pip install dvc dvc-s3`
4. setup remote:
    ```bash
   cd dvc
   dvc init
   dvc remote add -d versioning-article s3://<your-bucket-name>/dvc
   dvc remote modify versioning-article region us-west-2
    ```

## Run

`scalene src/main.py --dir=data`

## Tests

`scalene -m pytest tests`

`scalene src/main.py --dir=mock` # for quick testing  