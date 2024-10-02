# Use the official MySQL image as a base
FROM mysql:8.0

# Install yum
RUN microdnf install -y yum

# Update
RUN yum update -y

# Alias
RUN alias ll='ls -lha'

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
ADD . /app

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