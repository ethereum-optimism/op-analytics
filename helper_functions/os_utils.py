import os
import shutil
def clear_folder(folder_path):
        # Path to the directory you want to clear

        # Check if the directory exists
        if os.path.exists(folder_path):
        # List all files and directories in the folder
                for filename in os.listdir(folder_path):
                        file_path = os.path.join(folder_path, filename)
                        try:
                        # If it is a file, remove it
                                if os.path.isfile(file_path) or os.path.islink(file_path):
                                        os.unlink(file_path)
                                # If it is a directory, remove it and all its contents
                                elif os.path.isdir(file_path):
                                        shutil.rmtree(file_path)
                        except Exception as e:
                                print('Failed to delete %s. Reason: %s' % (file_path, e))
                print("Folder cleared.")
        else:
                print("The directory does not exist.")
