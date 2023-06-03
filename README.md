# DS4A_Team_14_Capstone
This is a repository for any code related to the DS4A Data Engineering Data Swan pipeline project.

## Key Instructions for use:
1. A config.yaml must be added to the root folder in order to credential into a remote cloud object storage solution.  This is currently set up for AWS, with a plan to make more extensible.
2. The get_data.py file currently contains the main code for getting code from CMS sources into object storage.  This is captured in the final for-loop in the code.  This will be refactor such that the script is removed from the getter/load functions.
