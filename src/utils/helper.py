import os
import shutil
import subprocess
from loguru import logger
import pyxet
import duckdb
import os.path as path
import boto3
import fsspec
import time
import s3fs


class Helper:
    M1 = "m1"
    DVC = "dvc"
    LFS = "lfs"
    S3 = "s3"
    LAKEFS = "lakefs"
    XETHUB = 'xethub'
    LFS_S3 = "lfs-s3"
    LFS_GITHUB = "lfs-github"
    XETHUB_PY = "xethub-py"
    XETHUB_GIT = "xethub-git"

    def __init__(self):

        self.s3 = boto3.client('s3')
        self.xet_pyxet_repo = self._get_xet_repo(Helper.XETHUB_PY)
        self.xet_git_repo = self._get_xet_repo(Helper.XETHUB_GIT)
        self.fs_xet = pyxet.XetFS()
        self.fs_s3 = s3fs.S3FileSystem(anon=False)

    @staticmethod
    def _get_xet_repo(path: str):
        origins = subprocess.run(
            "git remote -v", shell=True, cwd=path, stdout=subprocess.PIPE
        ).stdout.decode()
        origin = origins.split("\n")[0].split("\t")[1].split(" ")[0].split("/")
        return f"xet://{origin[-2]}/{origin[-1].replace('.git', '')}/main"

    def dvc_upload(self, filepath: str):
        self._dvc_upload(filepath)
        return {'function': 'dvc upload', 'tech': 'dvc', 'merged': False}

    def lfs_s3_upload(self, filepath: str):
        self._lfs_upload(filepath, Helper.LFS_S3)
        return {'function': 'lfs s3 upload', 'tech': 'lfs', 'merged': False}

    def lfs_git_upload(self, filepath: str):
        self._lfs_upload(filepath, Helper.LFS_GITHUB)
        return {'function': 'lfs git upload', 'tech': 'lfs', 'merged': False}

    def pyxet_upload(self, filepath: str):
        self._xethub_upload(filepath)
        return {'function': 'pyxet upload', 'tech': 'xethub', 'merged': False}

    def gitxet_upload(self, filepath: str):
        self._xethub_upload(filepath, pyxet_api=False)
        return {'function': 'git-xet upload', 'tech': 'xethub', 'merged': False}

    def lakefs_upload(self, filepath: str):
        self._lakefs_upload(filepath)
        return {'function': 'lakefs upload', 'tech': 'lakefs', 'merged': False}

    def s3_upload(self, filepath: str):
        self._s3_upload(filepath)
        return {'function': 's3 new upload', 'tech': 's3', 'merged': False}

    def s3_copy_time(self, local_path: str, s3_path: str):

        with fsspec.open(local_path, 'rb') as f1:
            data = f1.read()
        start_time = time.time()
        self.fs_s3.open(s3_path, 'wb').write(data)
        end_time = time.time()
        return {'function': 's3 copy time', 'tech': 's3', 'merged': True, 'upload_time': end_time - start_time}

    def xet_copy_time(self, filepath: str, xetpath: str):
        xet_fs = pyxet.XetFS()
        with fsspec.open(filepath, 'rb') as f1:
            data = f1.read()
        print(f"Read data, size = {len(data)}")
        start_time = time.time()
        with xet_fs.transaction:
            with xet_fs.open(xetpath, 'wb') as f2:
                f2.write(data)
            f2.close()
        end_time = time.time()
        return {'function': 'xet copy time', 'tech': 'xethub', 'merged': True,
                'upload_time': end_time - start_time}

    def lakefs_copy_time(self, filepath: str):
        start_time = time.time()
        self._lakefs_upload(filepath)
        end_time = time.time()
        return {'function': 'lakefs merged upload', 'tech': 'lakefs', 'merged': True,
                'upload_time': end_time - start_time}

    @staticmethod
    def copy_file(filepath: str, repo: str):
        filename = os.path.basename(filepath)
        targetpath = os.path.join(repo, filename)
        shutil.copyfile(filepath, targetpath)
        return {'function': 'copy_file', 'filepath': filepath, 'tech': Helper.M1}

    def get_file_size(self, filepath):
        return self.to_mb(os.path.getsize(filepath))

    @staticmethod
    def to_mb(bytes: int):
        return bytes / (1024 * 1024)

    def _dvc_push(self):
        command = """
                  dvc push
                  """
        return self.run(command, Helper.DVC)

    def _git_push(self, repo: str = ''):
        command = """
                  git push
                  """
        return self.run(command, repo)

    def run(self, command: str, repo: str = ''):
        logger.debug(command)
        args = {"shell": True, "capture_output": True}
        if repo:
            args["cwd"] = repo
        out = subprocess.run(command, **args)
        logger.debug(out)
        return out

    def _dvc_add_commit(self, filename: str):
        command = f"""
                        dvc add {filename}
                        git add {filename}.dvc
                        git commit -m "commit {filename}"
                        git push                
                        """
        return self.run(command, Helper.DVC)

    def _dvc_upload(self, filepath: str):
        filename = os.path.basename(filepath)
        self._dvc_add_commit(filename)
        self._dvc_push()

    def _dvc_remove(self, path:str):
        if not path.startswith(Helper.DVC):
            path = os.path.join(Helper.DVC, path)
        command = f"""
                    dvc remote {path}
                    git rm -r {path}
                    rm -rf -r {path}
                    """
        return self.run(command, Helper.DVC)

    def _remove(self, path:str, repo:str):
        if repo == Helper.DVC:
            return self._dvc_remove(path)
        command = f"""
                    rm -rf -r {path}                                         
                    git add {path}
                    git commit -m "remove {path}"
                    git push       
                    """
        return self.run(command,repo)

    def _git_add_commit(self, filename: str, repo: str = '', commit: str = ""):
        commit = commit or filename
        command = f"""                                
                    git add {filename}
                    git commit -m "commit {commit}"
                    """
        return self.run(command, repo)

    def _git_upload(self, filepath: str, repo: str):
        filename = os.path.basename(filepath)
        self._git_add_commit(filename, repo)
        return self._git_push(repo)

    def _lfs_upload(self, filepath: str, lfs_dir: str):
        return self._git_upload(filepath, lfs_dir)

    def _xethub_upload(self, filepath: str, pyxet_api: bool = True):
        filename = os.path.basename(filepath)
        if pyxet_api:
            with self.fs_xet.transaction:
                self.fs_xet.put(filepath, f"{self.xet_pyxet_repo}/{filename}")
        else:
            self._git_upload(filepath, Helper.XETHUB_GIT)
        return pyxet_api

    def output_upload(self, commit: str):
        self._git_add_commit('output/*', commit=commit)
        self._git_push()

    def merge_files(self, new_filepath: str, merged_filepath: str):
        if not path.exists(merged_filepath):
            shutil.copyfile(new_filepath, merged_filepath)
        else:
            duckdb.execute(
                f"""
                        COPY (SELECT * FROM read_parquet(['{new_filepath}', '{merged_filepath}'])) TO '{merged_filepath}' (FORMAT 'parquet');
                        """
            )
        return {'filename': new_filepath, 'tech': Helper.M1}

    def git_exists(self, path: str, cwd: str):
        return self.run(f"git cat-file -e origin:{path}", repo=cwd).returncode == 0

    def git_remove(self, path: str, cwd: str):
        command = f"""
        git rm {path}
        git commit -m "remove {path}"
        git push
        """
        self.run(command, repo=cwd)

    def xet_ls(self, xet_repo: str):
        return self.fs_xet.ls(xet_repo, detail=False)

    def xet_remove(self, filename: str, xet_repo: str):
        with self.fs_xet.transaction:
            self.fs_xet.rm(f"{xet_repo}/{filename}")

    def _lakefs_upload(self, filepath):
        self.run(f"lakectl fs upload -s {filepath} lakefs://versioning-article/main/{filepath}")

    def _s3_upload(self, filepath):
        self.run(f"aws s3 cp {filepath} s3://versioning-article/s3/{filepath}")

    def s3_file_count(self, prefix: str = ''):
        response = self.s3.list_objects_v2(Bucket='versioning-article', Prefix=prefix)
        return response['KeyCount']

    def s3_remove(self, filepath: str):
        self.run(f"aws s3 rm s3://versioning-article/s3/{filepath}")

    def s3_file_exists(self, filepath: str):
        try:
            self.s3.head_object(Bucket='versioning-article', Key=f's3/{filepath}')
            return True
        except Exception:
            return False
