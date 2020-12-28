# build with:
# docker build -t nba_jupyter .

# https://u.group/thinking/how-to-put-jupyter-notebooks-in-a-dockerfile/
FROM ubuntu:latest

# ENV

# set TERM so clear works
ENV TERM=cygwin

# set display port to avoid crash
ENV DISPLAY=:99

# set timezone
ENV TZ=America/Chicago
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# apt-get

RUN apt-get update && apt-get -y update
RUN apt-get install -y build-essential python3.6 python3-pip python3-dev

# https://mariadb.com/kb/en/about-mariadb-connector-c/
RUN apt-get install -y libmariadb3 libmariadb-dev

# Tini
# https://u.group/thinking/how-to-put-jupyter-notebooks-in-a-dockerfile/
# Add Tini. Tini operates as a process subreaper for jupyter. This prevents kernel crashes.
ENV TINI_VERSION v0.6.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
RUN chmod +x /usr/bin/tini
ENTRYPOINT ["/usr/bin/tini", "--"]

# install python libs

RUN pip3 install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir jupyter
# need to install jupyter here before we can set password

# set jupyter notebook password
RUN jupyter notebook --generate-config
RUN echo "c.NotebookApp.password='sha1:d6d6bf9eb171:2842d65865cf1374d54677328b72fa4e8f6f1513'" >> /root/.jupyter/jupyter_notebook_config.py

# set up working dir
RUN mkdir -p /home/carbo

# install python libs

COPY requirements.txt /tmp
RUN pip install --no-cache-dir -r tmp/requirements.txt

# jupyter
# https://u.group/thinking/how-to-put-jupyter-notebooks-in-a-dockerfile/
# https://stackoverflow.com/questions/47092878/auto-configure-jupyter-password-from-command-line
CMD ["jupyter", "notebook", "--port=8888", "--no-browser", "--ip=0.0.0.0", "--allow-root"]