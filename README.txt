## build jar file
in `my-flink-project` directory
```sh
mvn clean install -DskipTests -Dfast -Pskip-webui-build -T 1C
```

## submit job to flink
```sh
./bin/flink run -d ./pg/my-flink-project/target/my-flink-project-0.1.jar
```
