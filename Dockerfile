# Use the base image as the OpenCPU-base which is the Official OpenCPU docker file
FROM opencpu/base

# Copy all the files to /usr/local/src/app directory
# R/ : R script is placed in this directory.
# docker/ : it has installer.R which has list of libraries and server.conf file.
# test: The master test script that have code for converting the JSON format to base64.
# DESCRIPTION: Metadata about the R package you are developing.
# NAMESPACE: R package instructions for importing other libraries and exporting your own functions.
# man/ : It has the .Rd file for our script.
COPY . /usr/local/src/app


# Move OpenCPU configuration files to /etc/opencpu/.  The server.conf file is a JSON format file which is used
# to tell OpenCPU about the R dependencies in advance, so it will preload those libraries at server startup.
COPY docker/opencpu_config/* /etc/opencpu/

# Set the working directory
WORKDIR /usr/local/src

# Run installer.R script to install R dependencies
RUN /usr/bin/R --vanilla -f app/docker/installer.R

# Parameterize the Package Name
ARG PackageName


# Convert the package into bundled form which is tar.gz format
RUN tar czf /tmp/$PackageName.tar.gz app/


# Install the code as an R package on the OpeCPU server
RUN /usr/bin/R CMD INSTALL /tmp/$PackageName.tar.gz
