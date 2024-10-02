
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