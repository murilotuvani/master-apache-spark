# Empresa do fornecedor do curso
https://jobreadyprogrammer.com/

# Repositorio com o exemplos do curso prontos
https://github.com/imtiazahmad007/sparkwithjava

# Iniciando o servidor com docker
docker run -d --name spark <s>-p 5432:5432</s> -p 7077:7077 -e SPARK_MODE=master bitnami/spark</br>
docker run -d --name spark -p 4040:4040 -p 7077:7077 -e SPARK_MODE=master bitnami/spark

# Se quiser utilizar o PostreSQL
docker run --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres
docker exec -it postgres /bin/bash
create database spark_course_data;

## Documentação da imagem do docker
https://hub.docker.com/r/bitnami/spark

### Para reduzir o logging
1). Add the following imports to the top of the class that contains your main method.

    import org.apache.log4j.Logger;
    import org.apache.log4j.Level;


2). After that, inside your main method, add the following 2 lines.

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);