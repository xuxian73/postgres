FROM ubuntu:22.04
RUN apt-get update
RUN apt-get install -y python3 git vim wget build-essential
