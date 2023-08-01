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

1. `git xet clone https://xethub.com/xdssio/versioning-xethub-v2.git xethub` # use your own repository
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
5. `git lfs track '*.parquet'`
6. Create a *.lfsconfig* file a url to our local server:
   ```bash
   echo "[lfs]
    url = http://localhost:9999/git-lfs/blog.dermah.com.git" > .lfsconfig
   ```

7. Edit the file in 'lfs-server/config/default.json:
    ```json
    ...
    "store": {
     "type": "s3",
     "options": {
         "bucket": "<your bucket>",
         "region": "<your region>"
     }
    ```
8. Run local
   lfs-server
   ```
   cd lfs-server
   npx node-git-lfs
   # if you get problems try:
   npx "github:Dermah/node-git-lfs#4b79bee4"
   ```

* Thanks to [@Sputnik/Dermah](https://blog.dermah.com/2020/05/26/how-to-be-stingy-git-lfs-on-your-own-s3-bucket/)

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

```bash
export PYTHONPATH="$(pwd):$PYTHONPATH"
python src/download.py --dir=data --download=all --limit=40

python src/main.py --dir=data
# recommended:
git add output.csv profile.prof
git commit -m "upload results"
git push
```


## Tests

```

pytest tests

python src/generate.py --dir=mock --count=10 --rows=10
python src/main.py --dir=mock # for quick testing

# or
export PYTHONPATH="$(pwd):$PYTHONPATH" 
python src/main.py --dir=mock --rows=10000

```

## Analyse results

```bash
snakeviz profile.prof
```

`  