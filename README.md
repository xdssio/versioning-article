# versioning-article

## Setup

### S3 setup

> TODO

### Setup repositories

```
git xet clone https://xethub.com/xdssio/versioning-xethub.git
git clone https://github.com/xdssio/versioning-dvc.git
cd dvc
dvc init
dvc remote add -d versioning-article s3://<your-bucket-name>/dvc
dvc remote add -d versioning-article s3://versioning-article/dvc
dvc add titanic.parquet
git add .
git commit -m "add titanic.parquet"
git push
dvc push 
## forgeting to add and remove

## A remarke about deleting afile without deleting it's history
## Very hard to delete - why no secrets to git, no upload files to normal git. - reference by Ajit
# writen in rust vs go(lfs) vs DVC(python)
# LakeFS aside, annex aside.


1. Multipl Append
2. Add multiple files
* taxi
* titanic
3. dedup - add the same file twice -> everyone should catch it

``` 

[Benchmarks - ripgrep](https://blog.burntsushi.net/ripgrep/)

### Repositories

Create a XetHub repository and three github repositories for DVC, git LFS and git-annex with readme's.
Here I named them `versioning-xethub`, `versioning-dvc`, `versioning-lfs` and `versioning-annex`.

Add Them as submodluels to this repository:

```bash
git xet clone https://xethub.com/xdssio/versioning-xethub xethub
git clone https://github.com/xdssio/versioning-dvc dvc
git clone https://github.com/xdssio/versioning-lfs lfs
# git clone https://github.com/xdssio/versioning-annex annex
```

### Git LFS

#### Setup

1. [Install CLI](https://github.com/git-lfs/git-lfs?utm_source=gitlfs_site&utm_medium=installation_link&utm_campaign=gitlfs#installing)
2. `git clone https://github.com/xdssio/versioning-lfs.git lfs`
3. `cd lfs`
4. `git lfs install`
5. Create a *.lfsconfig* file with:
   ```yaml
    [lfs]
    url = https://<your-bucket-name>.s3.amazonaws.com/lfs
    ``` 
7. `git lfs track .`

* Deciding which files to track or folders to track is a bit of a pain. But in many cases you can simply use a specific
  folder for models/data or define a file types based on extensions.
* A tool
  like [git-lfs-migrate](https://github.com/git-lfs/git-lfs/blob/main/docs/man/git-lfs-migrate.adoc?utm_source=gitlfs_site&utm_medium=doc_man_migrate_link&utm_campaign=gitlfs)
  can track files based on size.
    * `git lfs migrate import --include="*.pdf" --size=100m`

#### Add a file

Pretty much like adding any other file to git given it is tracked by git lfs.

1. `cp data/titanic.parquet lfs/titanic.parquet`
2. `cd lfs`
3. `git add lfs/titanic.parquet`
4. `git commit -m "add titanic.parquet"`
5. `git push`

### XetHub setup

1. [Install CLI](https://xethub.com/assets/docs/getting-started/installation)
2. [Get token](https://xethub.com/user/settings/pat) and setup as environment variables:
    ```bash
      export XET_USER_NAME=<user-name>
      export XET_USER_TOKEN=<xethub-token>
    ```

3.`pip install pyxet`

#### Add

Pretty much like adding any other file to git. XetHuB consider the file size and optmize for it.

1. `cp data/titanic.parquet xethub/titanic.parquet`
2. `cd xethub`
2. `git add titanic.parquet`
3. `git commit -m "add titanic.parquet"`
4. `git push`

### DVC

#### setup

1. [Install CLI](https://dvc.org/doc/install)
2. `pip install dvc`
3. `dvc init`
4. `dvc remote add -d versioning-article s3://<your-bucket-name>/dvc`
5. `dvc remote modify versioning-article region us-west-2`

#### Add a file

```bash
1. cp data/titanic.parquet dvc/titanic.parquet
2. cd dvc
3. dvc add titanic.parquet # mark file for dvc
4. git add . # add meta files
5. git commit -m "add titanic.parquet"
6. git push 
7. dvc push # add actual file to s3
```

### Annex setup

**In the annex directory - or a mess will ensue**

* [Install CLI](https://git-annex.branchable.com/install/)
* `cd annex`
* `git annex init`
* `git annex initremote s3 type=S3 encryption=none bucket=<your-bucket-name> fileprefix=annex`

### Datalad

1. [Install CLI](https://www.datalad.org/#install)
2.

# Benchmarks

## Setup

```bash
python -m venv .venv \
&& source .venv/bin/activate \
&& pip install -r requirements.txt

# Download data - takes time! 
python src/download.py --dir=data --download=all --limit=2

# Test
python src/.py --dir=data --download=all --limit=2
```

## Run

scalene src/main.py mock

# Tests

scalene -m pytest tests
scalene tests/merge_test.py
scalene src/main.py mock  