# mysql.migrator
[![Python3.12.4](https://img.shields.io/badge/Python-3.12.4-f4d159+green)](https://www.python.org/downloads/release/python-3124/) 
![OS](https://img.shields.io/badge/Tested%20On-Linux%20|%20Windows11-yellowgreen.svg?style=flat-square)
![Status](https://img.shields.io/badge/status-beta-orange)
[![License](https://img.shields.io/badge/License-MIT-blue.svg?style=flat-square)](https://github.com/Manisso/fsociety/blob/master/LICENSE)
[![Hits](https://hits.sh/github.com/skntbcn/docsyn.svg?color=dfb317)](https://hits.sh/github.com/skntbcn/docsyn/)


# MySQL Data Migration Tool

A Python-based tool for efficiently migrating table data between MySQL instances, with dynamic batch sizing, tqdm progress bars and multithreading executions.
Being aware that there are already many MySQL migration tools (including a simple mysqldump), the goal of this project was always to learn Python. Nevertheless, the result is useful for anyone who needs a fast way to migrate data between MySQL instances with process progress tracking.

## An Apology to Pythonistas 🫶

Before you dive into the code, I owe an apology to the seasoned Python programmers out there. This project might be what you'd call a "beginner's ode to Python." I apologize if the code offends your refined senses, and I welcome any suggestions or contributions to help improve it! Remember, it's my second python script, I'm still learning 😊

## Features

- **Batch-Based Migration**: Migrate table data in manageable batches to optimize performance and reduce memory usage.
- **Dynamic Batch Size Adjustment**: Automatically adjust batch sizes based on performance metrics and database characteristics (e.g., large BLOBs).
- **Primary Key Management**: Handle tables with or without primary keys, adapting the migration strategy accordingly.
- **Progress Monitoring**: Visual progress tracking for each table migration process using a progress bar.
- **Auto-Reconnection**: Automatically reconnects and retries operations in case of connection loss or server issues.

## Getting Started

### Prerequisites

Before you begin, ensure you have the following installed:

- **Python 3.12.4+**
- **MySQL Connector for Python**
- **tqdm** (for progress bars)
- **colorama** (for terminal color formatting)

Install the required packages using:
```bash
pip install -r requirements.txt
```

### Installation

 1. Clone the repository:
 ```bash
 git clone https://github.com/skntbcn/mysql.migrator.git
 cd mysql.migrator
 ```

 2. Configure your MySQL source and destination database settings in the **connections.py** file. This configuration may work with deployed containers by using **CreateEnvironment.ps1**.
 ```python
# Source database config
source_config = {
    'user': 'user',
    'password': 'password',
    'host': '127.0.0.1',
    'port': 3307,
    'compress': True,
    'buffered': True
}

# Destination database config
destination_config = {
    'user': 'user',
    'password': 'password',
    'host': '127.0.0.1',
    'port': 3308,
    'compress': True,
    'buffered': True
}
 ```

### Deploy demo containers
Inside folders db-source and db-target, you can find both Dockerfiles which you can use to generate your own environment for testing database migrations.
In the db-target container, there will be a simple MySQL service up and running. In db-source, we will have the same setup as db-target, but 5 sample databases will be downloaded during the Docker image build. These are:
 - https://downloads.mysql.com/docs/airport-db.tar.gz
 - https://github.com/datacharmer/test_db.git
 - https://downloads.mysql.com/docs/world-db.zip
 - https://downloads.mysql.com/docs/menagerie-db.zip
 - https://downloads.mysql.com/docs/sakila-db.zip

To automatically build and run both Docker images, you can use **CreateEnvironment.ps1** script. It essentially does the following:
```powershell
# Stop running containers and remove them
docker stop $(docker ps --all -q)
docker rm -f $(docker ps --all -q)

# Remove all images and other things
docker image remove $(docker images --all -q)
docker system prune --all

# Build
docker build -t mysql-source:latest .\db-source
docker build -t mysql-target:latest .\db-target

# Run both
docker run --detach --name mysql-source -p 3307:3306 mysql-source:latest
docker run --detach --name mysql-target -p 3308:3306 mysql-target:latest

# Wait 15 seconds until mysql starts
$Counter = 0
$TotalSeconds = 60
for ($i = 0; $i -lt $TotalSeconds; $i++) {
    $Progress = [math]::Round(100 - (($TotalSeconds - $Counter) / $TotalSeconds * 100));
    
    Write-Progress -Activity "Waiting for MySQL to start..." -Status "$Progress% Complete:" -SecondsRemaining ($TotalSeconds - $Counter) -PercentComplete $Progress
    Start-Sleep 1
    $Counter++
}

# Grants
docker exec -it mysql-source mysql -uroot -ptoor -e "GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' WITH GRANT OPTION; FLUSH PRIVILEGES;"
docker exec -it mysql-target mysql -uroot -ptoor -e "GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' WITH GRANT OPTION; FLUSH PRIVILEGES;"

# Import data in source
docker exec -it mysql-source /bin/bash -c "/app/import.sh"
```

Note that the last line will run a script located in the source container at /app/import.sh. This will run a simple bash script that loads the downloaded sample databases into MySQL.
This way, you will have a prepared environment to test MySQL migrations between these two containers.

## Usage

### Simple method

Run it without args.
```bash
# Execute with all options by default
python migrate.py

# The tool will migrate the data from the source MySQL instance to the destination, displaying progress automatically.
```

### Command-Line Arguments

You can pass various options to customize the migration process:

```bash
# Optional. Sets the number of records to migrate in each (Default is 2048)
-b BATCH_SIZE, --batch-size BATCH_SIZE

# Optional. Skip migration of databases that already exist on the destination. By default, all databases are migrated.
-s, --skip-existing-dbs

# Optional. Keep existing databases on the destination before migration. By default, destination databases are dropped before starting migration.         
-d, --keep-existing-dbs

# Optional. Migrate grants between MySQL instances. By default, grants are not migrated unless this flag is set.
-g, --migrate-grants  

# Optional. Number of threads to use for database migration. Default is to use all CPU cores except one.
-t THCOUNT, --thread-count THCOUNT

# Optional. Only check the last migration process. No changes will be made
-c, --check-only
```

### Some examples

Some examples you may use:

```bash
# Reduce the default 2048 batch size to 512
python migrate.py --batch-size 512

# Avoid deleting target databases during migration
python migrate.py --skip-existing-dbs

# Change thread count from default value, which is (cpu.count - 1)
python migrate.py --thread-count 2

# Performs only a check from last migration process
python migrate.py --check-only
```



## Contributing
Contributions to this project are welcome! Here are some ways you can contribute:
 - Submitting bug reports and feature requests.
 - Improving the documentation.
 - Submitting pull requests to help improve the code.
 - License
 - Distributed under the MIT License. See LICENSE for more information.

Any contributions are welcome!

## Screenshots

Screenshot 1:
![Sample 1](/img/1.png?raw=true "Sample 1")

Screenshot 2:
![Sample 2](/img/2.png?raw=true "Sample 2")

Screenshot 3:
![Sample 3](/img/3.png?raw=true "Sample 3")


## Contact ✨
skntbcn – @skntbcn - skntbcn@gmail.com
Project Link: https://github.com/skntbcn/mysql.migrator

For any questions or issues, feel free to open an issue on GitHub or reach out to me at skntbcn@gmail.com. We hope you find this tool useful for your projects and look forward to seeing how it can evolve with community contributions! ❤️
