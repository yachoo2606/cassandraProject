# cassandraProject

## RUN

to run this project please type this command in main project directory
```shell
docker compose down --volumes && docker compose build --no-cache && docker compose up --force-recreate -d
```

to generate visualization of network in docker use in directory which contains docker-compose.yml file
```shell
docker run --rm -it --name dcv -v $(pwd):/input pmsipilot/docker-compose-viz render -m image docker-compose.yaml
```