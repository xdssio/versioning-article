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
    XETHUB = "xethub"

    def __init__(self):
        origins = subprocess.run(
            "git remote -v", shell=True, cwd="xethub", stdout=subprocess.PIPE
        ).stdout.decode()
        origin = origins.split("\n")[0].split("\t")[1].split(" ")[0].split("/")
        self.fs = pyxet.XetFS()
        self.xet_repo = f"xet://{origin[-2]}/{origin[-1].replace('.git', '')}/main"

    def dvc_upload_new(self, filepath: str):
        return self._dvc_upload(filepath)

    def dvc_upload_merged(self, filepath: str):
        return self._dvc_upload(filepath)

    def lfs_s3_upload_new(self, filepath: str):
        return self._lfs_upload(filepath, Helper.LFS_S3)

    def lfs_s3_upload_merged(self, filepath: str):
        return self._lfs_upload(filepath, Helper.LFS_S3)

    def lfs_github_upload_new(self, filepath: str):
        return self._lfs_upload(filepath, Helper.LFS_GITHUB)

    def lfs_github_upload_merged(self, filepath: str):
        return self._lfs_upload(filepath, Helper.LFS_GITHUB)

    def xethub_upload_new(self, filepath: str):
        return self._xethub_upload(filepath)

    def xethub_upload_merged(self, filepath: str):
        return self._xethub_upload(filepath)

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
                self.fs.put(filepath, f"{self.xet_repo}/{filename}")
        else:
            if filename not in set(pathlib.Path(Helper.XETHUB).glob("*")):
                self.copy_file(filepath, f"{Helper.XETHUB}/{filename}")
            self._git_upload(filepath, Helper.XETHUB)
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
        return self.fs.ls(self.xet_repo, detail=False)

    def xet_remove(self, filename: str):
        with self.fs.transaction:
            self.fs.rm(f"{self.xet_repo}/{filename}")
