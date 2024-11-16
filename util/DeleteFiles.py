import os

def delete_files(directory, flag):
    """Delete files from the specified directory if the flag is True."""
    if flag:
        try:
            # List all files in the directory
            files = os.listdir(directory)
            
            # Loop through each file and delete it
            for file in files:
                file_path = os.path.join(directory, file)
                
                # Check if it's a file and delete it
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    print(f"Deleted file: {file}")
            
            print("All files deleted successfully." if files else "No files to delete.")
        
        except Exception as e:
            print(f"Error while deleting files: {e}")
    else:
        print("Flag is not True. No files were deleted.")
