## Custodian Service Obfuscation Manager

Script allows to obfuscate and de-obfuscate data aggregated by Custodian Service while scanning infrastructure. 
## Installation

### Prerequisites
1. The following software must be installed in order to complete the guide: 
* python3.8 or higher
* pip (installation guide: https://pip.pypa.io/en/stable/installation/)
* virtualenv (to install: `pip install virtualenv`)

2. Create virtualenv using the command: 
```bash
virtualenv -p python3 venv
```
3. Activate newly created virtualenv with the command
* Linux/Mac: 
```bash
source venv/bin/activate
```
* Windows CMD: 
```bash
venv/Scripts/activate.bat
```
4. Install required modules withing the virtualenv: 
* Linux/Mac/Windows CDM: 
```bash
pip install .
```

5. Installation is done.

## Obfuscation flow
1. Execute the following command: 
```bash
sreobf obfuscate --dump-directory "$custodian_dump_folder" --to "$target_folder" --dictionary-out "$obfuscation_dictionary.json"
```
Where: 
* $custodian_dump_folder - is the full path to the folder where Custodian Service dump is stored
* $target_folder - is the full path to the folder where the obfuscation result will be stored. The folder will be created if does not exist
* $obfuscation_dictionary.json - is the name of the file where the mapping of resource names to synthetic ids will be stored. Please keep it safe - it is impossible to de-obfuscate data without this file

2. Obfuscation done. 

## Deobfuscation flow
1. Execute the following command: 
```bash
sreobf deobfuscate --dump-directory "$obfuscated_data_folder" --dictionary "$objuscation_dictionary.json" --to "$deobfuscated_data_folder"
```
Where: 
* $obfuscated_data_folder - is the full path to the folder where Custodian Service obfuscated data is stored
* $obfuscation_dictionary.json - is the name of the file where the mapping of resource names to synthetic ids will be stored
* $deobfuscated_data_folder - is the full path to the folder where the de-obfuscation result will be stored. The folder will be created if does not exist
**Note:** A value of parameter `--to` can be omitted. If it's omitted it will become the same as `--dump-directory`
2. Deobfuscation done.