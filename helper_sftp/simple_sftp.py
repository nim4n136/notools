import os
import pysftp

class SimpleLibSftp:

    cnopts = pysftp.CnOpts()
    filenames  = []
    dst_root = "storage"

    in_text = False

    def __init__(self):
        self.cnopts.hostkeys = None

    def set_dst_root(self, dst):
        self.dst_root = dst

    def store_files_name(self, name):
        if self.in_text:
            if self.in_text in name:
                self.filenames.append(name)    
        else:
            self.filenames.append(name)

    def store_dir_name(self, name):
        pass

    def store_other(self, name):
        pass

    def walkDownload(self,cwd_dir, host, username, password):
        sftp = pysftp.Connection(host, username=username, password=password,cnopts=self.cnopts)
        sftp.walktree(cwd_dir,self.store_files_name,self.store_dir_name,self.store_other,recurse=True)

        for filename in self.filenames:
            dst = filename.replace(cwd_dir,"")
            dst = dst.replace("/","__")
            self._getDownload(sftp, filename, f"{self.dst_root}/{host}/{dst}")
            print(f"Success {host} {filename}")

    def _getDownload(self,sftp, filename, dst):
        sftp.get(filename, dst)
        



    
    



