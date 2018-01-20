//Teste por Thiago Luis Bardella dos Santos



// Um diretório de input/ deve ser criado para alocar os arquivos de entrada

val textFile = sc.textFile ("input/")

val validLines = textFile.map(_.split(" ")).filter{_.size == 10}
	
// 1. Número de hosts únicos.

//Trago a primeira coluna e com contador
val colHosts = textFile.map(_.split(" ")).map(column=>(column(0),1))

//Reduzo todos os casos repetidos
val hostsCounted = colHosts.reduceByKey(_ + _)

//trazer apenas os campos com contador em 1, ou seja, hosts unicos
val uniqueHosts = hostsCounted.filter(_._2 == 1)

//Contar o número de linhas
val countUniqueHosts = uniqueHosts.count()



// 2. O total de erros 404.

//apenas as linhas que contenham "404" 
val lines404 = textFile.map(_.split(" ")).filter(_.contains("404"))

//Trago apenas a coluna de Status
val colStatus = lines404.map(column=>(column(8),1))

//trazer apenas os campos com status 404
val status404 = colStatus.filter(_._1 == "404")

//Contar o número de linhas
val countStatus404 = status404.count()



// 3. Os 5 URLs que mais causaram erro 404.

// Aproveitar as linhas que contenham "404" e trazer a requisição com o status
val reqMaisStatus = lines404.map(column=>(column(5) + " " + column(6) + " " + column(7),column(8),1))

//trazer apenas os campos com status 404
val reqStatus404 = reqMaisStatus.filter(_._2 == "404")

//Mapeamento para realizar contagem
var reqStatus404map = reqStatus404.map({case (a,b,c)=>((a,b),c)})

//Reduzo todos os casos repetidos
reqStatus404map = reqStatus404map.reduceByKey(_ + _)

//Ordenar de forma decrescente todos os resultados para tomar aqueles com maior contagem
reqStatus404map.sortBy(_._2,false).take(5).foreach(println)

// 4. Quantidade de erros 404 por dia.

//Aproveitar as linhas que contenham "404" e trazer o TIMESTAMP(apenas a data - de index 1  a 12)
val timestampMaisStatus = lines404.map(column=>(column(3).substring(1,12),column(8),1))

//Trazer apenas os campos com status 404
val dateStatus404 = timestampMaisStatus.filter(_._2 == "404")

//Mapeamento para realizar contagem
var dateStatus404map = dateStatus404.map({case (a,b,c)=>((a,b),c)})

//Reduzo todos os casos repetidos
dateStatus404map = dateStatus404map.reduceByKey(_ + _)

//Retornar o número de erros 404 com a data referente
dateStatus404map.foreach(println)

// 5. O total de bytes retornados.

//Trazer coluna que tenha a quantidade de bytes 
val bytes = validLines.map(column=>(column(8), column(9)))

//Trazer apenas os campos com status diferente de 404
val bytesValidos = bytes.filter(_._2 != "-")

val bytesINT = bytesValidos.map({case (a,b) => (b.toInt)})

//Somar todos os valores

val bytesSum = bytesINT.reduce(_ + _)

