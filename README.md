# master-apache-spark
Anotations and source code utilized in the course https://www.udemy.com/course/the-ultimate-apache-spark-with-java-course-hands-on



#Conversão de dataset para dataframe e vice versa.
Dataset<T> - tem classes java, mas não consegue fazer as operações.<br/>
Dataset<Row> - Somente Row, mas consegue fazer as operações sobre os registros. 

##Funções - projeto 4
### map()
map() - Entra um elemento e sai um elemento, um map() sobre as letras do alfabeto que transformem eles em seus números ordinais fica:<br/>
| x | f(x)->y | y |
| - | ------- | - |
| A | -> | 1 |
| B | -> | 2 |
| C | -> | 3 |
| D | -> | 4 |


### reduce()
reduce() a função reduce é aplicada a todo o conjunto e tem como resultado apenas um valor.<br/>

| x | f(x)->y | y |
| -: | :-------: | -: |
| 2 |  -> |  |
| 2 |  -> |  |
| 2 |  -> | 8 |
| 2 |  -> |  |
 

### MapFunction
A interface MapFunction serve para transformar um Row em um Pojo e vice versa.

### flatMap()
flatMap() pega uma entrada e produz diversas saídas.
Exemplo, ter como a entrada um texto e na saida a contagem de palavras do texto.



## Projeto 5
### Agregações, transformações, ações e DAG.
O ato de adiconar ou remover, fazer junções são transformações.
Agrupamentos 

show() Exibir é uma ação.
collect()
collectAsList() é ação
describe() estatísticas, é uma ação
map() e reduce() são ações.

As transformações não acontecem antes de seja criada uma ação.
Quando uma ação é enviada então o DAG (Directed Acyclic Graph) faz as ações de maneira procedural.
DAG é um grafo não ciclico direcional, isto é, só tem uma direção.


# Acessando o Spark servidor e não a versão embutida
URL de conexão: spark://master_hostname:port

### Se tiver múltiplos servidores:
addresses: spark://master1_hostname:port1,master2_hostname:port2

## Streaming
Há várias maneiras de ingerir os dados e também de disponibilizar as informações.
[Guia de programção difusão estruturada _structured streamin_](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

## Machine Learning com Spark
Um livro recomenado para leitura é [An Introduction to Statistical Learning](https://www.statlearning.com/)

### Regressão Linear - Linear regression
Regressão linear, uma linha com a distância minima entre os pontos de um gráfico.
Dá para fazer regressão linear no Excel.
Tem regressão linar simples é que á relação entre f(x)=y, só tem uma variável.

### Regressão logistica
É um calculo de classificação, onde várias entradas geram uma saída booleana, o resultado computado pode ser 0,7 então o resultado é 1, caso o resultado computado seja 0,3 então o resultado será 0.

### K-Mean
Eh um metodo de classificar em um deteminado conjunto de clusteres os itens observados, onde o número de clusteres é K.
Então se você quer classificar em 3 clusters k=3, se quer 5 clusters então k=5.



