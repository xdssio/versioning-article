import os
import pathlib
import shutil
import subprocess
from loguru import logger
import pyxet
import duckdb
import os.path as path


class Helper:
    DVC = "dvc"
    LFS_S3 = "lfs-s3"
    LFS_GITHUB = "lfs-github"
    LFS = "lfs"
    XETHUB_PY = "xethub_pyxet"
    XETHUB_GIT = "xethub_git"
    XETHUB = 'xethub'
    LAKEFS = "lakefs"
    S3 = "s3"
    M1 = "m1"

    def __init__(self):

        self.fs = pyxet.XetFS()
        self.xet_pyxet_repo = self._get_xet_repo(Helper.XETHUB_PY)
        self.xet_local_repo = self._get_xet_repo(Helper.XETHUB_GIT)

    @staticmethod
    def _get_xet_repo(path: str):
        origins = subprocess.run(
            "git remote -v", shell=True, cwd=path, stdout=subprocess.PIPE
        ).stdout.decode()
        origin = origins.split("\n")[0].split("\t")[1].split(" ")[0].split("/")
        return f"xet://{origin[-2]}/{origin[-1].replace('.git', '')}/main"

    def dvc_new_upload(self, filepath: str):
        return self._dvc_upload(filepath)

    def dvc_merged_upload(self, filepath: str):
        return self._dvc_upload(filepath)

    def lfs_s3_new_upload(self, filepath: str):
        return self._lfs_upload(filepath, Helper.LFS_S3)

    def lfs_s3_merged_upload(self, filepath: str):
        return self._lfs_upload(filepath, Helper.LFS_S3)

    def lfs_git_new_upload(self, filepath: str):
        return self._lfs_upload(filepath, Helper.LFS_GITHUB)

    def lfs_git_merged_upload(self, filepath: str):
        return self._lfs_upload(filepath, Helper.LFS_GITHUB)

    def xethub_py_new_upload(self, filepath: str):
        return self._xethub_upload(filepath)

    def xethub_py_merged_upload(self, filepath: str):
        return self._xethub_upload(filepath)

    def xethub_git_new_upload(self, filepath: str):
        return self._xethub_upload(filepath, pyxet_api=False)

    def xethub_git_merged_upload(self, filepath: str):
        return self._xethub_upload(filepath, pyxet_api=False)


    def lakefs_new_upload(self, filepath: str):
        return self._lakefs_upload(filepath)

    def lakefs_merged_upload(self, filepath: str):
        return self._lakefs_upload(filepath)

    def s3_new_upload(self, filepath: str):
        return self._s3_upload(filepath)

    def s3_merged_upload(self, filepath: str):
        return self._s3_upload(filepath)

    @staticmethod
    def copy_file(filepath: str, repo: str):
        filename = os.path.basename(filepath)
        targetpath = os.path.join(repo, filename)
        shutil.copyfile(filepath, targetpath)
        return filename

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
            with self.fs.transaction:
                self.fs.put(filepath, f"{self.xet_pyxet_repo}/{filename}")
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
        return merged_filepath

    def xet_ls(self):
        return self.fs.ls(self.xet_pyxet_repo, detail=False)

    def xet_remove(self, filename: str):
        with self.fs.transaction:
            self.fs.rm(f"{self.xet_pyxet_repo}/{filename}")

    def _lakefs_upload(self, filepath):
        self.run(f"lakectl fs upload -s {filepath} lakefs://versioning-article/main/{filepath}")

    def _s3_upload(self, filepath):
        self.run(f"aws s3 cp {filepath} s3://versioning-article/s3/{filepath}")
