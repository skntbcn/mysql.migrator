# Use the official MySQL image as a base
FROM mysql:8.0

# Install yum
RUN microdnf install -y yum

# Update
RUN yum update -y

# Install packages
RUN yum install -y curl
RUN yum install -y wget
RUN yum install -y git
RUN yum install mysql-shell
RUN yum install vim
RUN yum install zip

# Alias
RUN alias ll='ls -lha'

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
ADD . /app

# Copy script
COPY import.sh /app/import.sh
RUN chmod +x /app/import.sh

# Download airport-db DB
RUN wget https://downloads.mysql.com/docs/airport-db.tar.gz
RUN tar xvzf airport-db.tar.gz

# Download test_db DB
RUN git clone https://github.com/datacharmer/test_db.git

# Download world-db DB
RUN wget https://downloads.mysql.com/docs/world-db.zip
RUN unzip world-db.zip

# Download menagerie-db DB
RUN wget https://downloads.mysql.com/docs/menagerie-db.zip
RUN unzip menagerie-db.zip

# Download sakila-db DB
RUN wget https://downloads.mysql.com/docs/sakila-db.zip
RUN unzip sakila-db.zip

# Set the default user for the MySQL image
USER root

# # Set environment variables for the new user
ENV MYSQL_DATABASE=data
ENV MYSQL_ROOT_PASSWORD=toor
ENV MYSQL_USER=user
ENV MYSQL_PASSWORD=password

# Expose port 3306 to allow connections to the database
EXPOSE 3306

# Start the MySQL server when the container is run
CMD ["mysqld"]