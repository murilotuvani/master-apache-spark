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



