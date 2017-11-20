// Importando as bibliotecas necessárias.
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.GraphLoader


// Criando um RDD para os vértices. Um número como iD, o nome da cidade e uma característica da cidade.
val users: RDD[(VertexId, (String, String))] =
  sc.parallelize(Array((0L, ("Magé", "Rota de passagem")), (1L, ("São Gonçalo", "Alto potencial")), (2L, ("Araruama", "Acessível")), (3L, ("Rio de Janeiro", "Grande centro urbano")), (4L, ("Rio das Ostras", "Balneário")), (5L, ("Friburgo", "Cidade Turística"))))

// Criando um RDD para as arestas. Nesse exemplo as arestas serão as distâncias entre as cidades.
val relationships: RDD[Edge[Int]] =
  sc.parallelize(Array(Edge(3L, 1L, 29),    Edge(5L, 3L, 141),
                       Edge(2L, 5L, 142), Edge(5L, 1L, 119),
                       Edge(3L, 0L, 63),   Edge(1L, 0L, 40)))
                       
// Construindo um grafo
val graph = Graph(users, relationships)

// Mostrando o número de Vertices (cidades) contidas no grafo.
val numCidades = graph.numVertices

//Mostrando o número de estradas ligando as cidades no grafo contruído.
val numEstradas = graph.numEdges

//Mostrando as relações entre vértices externos.
graph.outDegrees.collect.foreach(println(_))

//Mostrando as relações entre vértices internos.
graph.inDegrees.collect.foreach(println(_))

//Mostrando os triangulos formados entre vértices para cada vertice.
graph.triangleCount.vertices.collect.foreach(println(_))

//Mostrando o Grafo criado
graph.vertices.collect.foreach(println(_))

graph.triplets.map(triplet => triplet.srcAttr._1 + " está a " + triplet.attr + " Km de " + triplet.dstAttr._1).collect.foreach(println(_))

//Descobrindo quais as rotas com distâncias maiores que 100.
graph.edges.filter { case Edge(src, dst, prop) => prop > 100 }.collect.foreach(println)

// Vamos supor que a cidade de Magé tem um bloqueio na estrada. Então vamos criar um subgrafo removendo essa cidade dos vertices validos, bem como suas arestas.
val validGraph = graph.subgraph(vpred = (id, attr) => attr._1 != "Magé")

// Mostrando o subgrafo válido, sem a cidade de Magé.
validGraph.vertices.collect.foreach(println(_))

// Mostrando os tripletos válidos, ou seja, sem a cidade de Magé.
validGraph.triplets.map(triplet => triplet.srcAttr._1 + " está a " + triplet.attr + " Km de " + triplet.dstAttr._1).collect.foreach(println(_))

// Vamos criar um Ranqueamento das cidades, incluindo a cidade de Magé, para ver qual cidade teria mais importância em termo de conexões. 
// Usamos o comando PageRank.
val ranks = graph.pageRank(0.0001).vertices

// Mostraremos o resultado do ranqueamento envolvemdo todos os vértices.
val ranksByUsername = users.join(ranks).map {
  case (id, (username, rank)) => (username, rank)
}
// Mostrando o resultado
println(ranksByUsername.collect().mkString("\n"))

//Pelos resultados apresentados neste exemplo conclue-se que a Cidade de Magé possui o maior valor de ranqueamento, ou seja ela tem mais interrelações do que as outras.
//Em segundo lugar está São Gonçalo.
