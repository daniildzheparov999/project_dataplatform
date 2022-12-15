## Data Platform Learning Project 

Hello! This is a learning project related to development of data platform services.

Here you can find the short instruction how to implement this platform locally in your computer.

### Instruction

#### 1. Download and install Docker 

You can find it and download using this link https://www.docker.com/products/docker-desktop/ 


#### 2. Pull this repository 

Firstly, you need to make sure, that you have installed Git in ypur machine. Otherwise, you will need to install it. 

1. Open CMD or Terminal
2. Run command

$ git --version 

3. If you have git installed, you will see the version number

Or Install git using this source https://git-scm.com/book/en/v2/Getting-Started-Installing-Git 

5. Open Visual Studio or other IDE 

6. Create some new folder for your project 

7. Open terminal and run command 

$ git pull https://github.com/daniildzheparov999/project_dataplatform.git 

After that you will have all code in your local machine 


#### 3. Install Application

Move to folder application 

$cd application

Run docker-compose.yaml file 

$ docker-compose up

You will get the application with postgres database, django backend and vue js frontend

Go into 'back_django' conatiner and execute commands 

$ python manage.py makemigrations 
$ python manage.py migrate
$ python manage.py runserver

Backend server should work

Go to http://localhost:8087/#/ to see the UI of the application, which should be avalible on the container 'front'

All data from the application will be stored in postgres database, to which you can connect using credentials from docker-compose.yaml file



#### 4. Install Clickhouse

Move to core project folder and run docker-compose.yaml file 

$ docker-compose up 

After that you will get running containers with clickhouse client and clickhouse server 

To connect to clickhouse you can use DBeaver and credentials from docker-compose.yaml file

HOST - localhost or the name of the container 'ch_server'
PORT - 8123 
USER - default 
PASSWORD - keep empty


#### 5. Install Airflow

Move to airflow folder and run docker-compose.yaml file 
$ cd airflow 
$ docker-compose up 

After initialization process you will get available and ready to use Airflow on http://localhost:8080 

You can use DAG 'etl_dag_v4' and trigger it to execute data from postgres database and load it into clickhouse

#### 6. Install Superset

Move to superset folder and run docker-compose.yaml 
$ cd superset 
$ docker-compose up

After container started, you need to to create and admin user and initialize database for superset.

Go to superset/init floder
Open superset-init.sh file 

Copy each command and run it in terminal

$ docker-compose exec -it superset superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin

$ docker-compose exec -it superset superset db upgrade

$ docker-compose exec -it superset superset init 

After that you will be able to login into superset



