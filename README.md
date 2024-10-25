# Facebook ETL Project (Middleware)

## Python Version
```
Python 3.9.12
```

## Deployment
- ### Code Deployment
    - Open git bash shell in project root directory
    - Initialize the git repo
        ```
        git init
        ```
    - Add remote repo as origin
        ```
        git remote add origin 
        ```
    - Authenticate the user
    - Pull the code from remote repo
        ```
        git pull origin master
        ```

- ### Create virtual environment
    - Run this command in project root directory
        ```
        python -m venv env
        ```
    - Activate the virtual environment
        ```
        source env/Scripts/activate
        ```
    - Install python package dependencies in virtual environment
        ```
        pip install -r requirements.txt
        ``` 

- ### Run any ETL pipeline from {project_root_dir}pipelines/{pipeline folder name}

- ### Eevery pipeline folder has following:
    - Logs folder: where all date level logs files (.log) will be created and updated when any pipleine gets executed
    - create_schema.pyw: This will create SQL table for pipleine in database. If table already exists then it will ignore table creation.
    - bulkimport.pyw: This pipeline will import the data in bulk. Need to just update the START_DATE and END_DATE constant values in bulkimport.pyw file.
    - cronjob.pyw: This will import the data to SQL table from days mentioned in DAYS constant in cronjob.pyw file from the currect date of execution. Usually its for previous day data update.