#!/bin/bash
set -e

# Update system packages
sudo apt-get update

# Install Java 17 (required for Kafka)
sudo apt-get install -y openjdk-17-jdk

# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> $HOME/.profile

# Add Java to PATH
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> $HOME/.profile

# Make gradlew executable
chmod +x ./gradlew

# Verify Java installation
java -version
javac -version

# Verify Gradle wrapper
./gradlew --version

echo "Environment setup completed successfully"