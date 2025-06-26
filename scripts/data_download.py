"""
This script downloads the Shanghai T2DM dataset from Zhao et al. (2023),
available at https://figshare.com/articles/dataset/Diabetes_Datasets-ShanghaiT1DM_and_ShanghaiT2DM/20444397?file=38259264.

The data will be saved in the ./data folder. Only keeps the used T2DM data from the corpus.
"""

import os
import zipfile
import requests
import shutil


download_folder = "./data"
zip_path = os.path.join(download_folder, "data.zip")


URL = "https://figshare.com/ndownloader/files/38259264"


# Download ZIP data
print(f"Downloading data to {zip_path}...")
response = requests.get(URL)
response.raise_for_status()
with open(zip_path, 'wb') as f:
    f.write(response.content)


# Unzip into ./data
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(download_folder)

# Locate the inner folder
inner_folders = [name for name in os.listdir(download_folder)
                 if os.path.isdir(os.path.join(download_folder, name)) and name != "Shanghai_T2DM"]
if not inner_folders:
    raise RuntimeError("No inner folder found after unzipping.")
inner_path = os.path.join(download_folder, inner_folders[0])


#Move desired files/folders to ./data
items_to_keep = ["Shanghai_T2DM", "Shanghai_T2DM_Summary.xlsx"]
for item in items_to_keep:
    src = os.path.join(inner_path, item)
    dst = os.path.join(download_folder, item)
    if os.path.exists(dst):
        shutil.rmtree(dst) if os.path.isdir(dst) else os.remove(dst)
    shutil.move(src, dst)

# Clean up
shutil.rmtree(inner_path)  # remove unzipped subfolder
os.remove(zip_path)        # remove zip file
print("Files saved in ./data")


