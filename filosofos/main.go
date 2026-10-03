package main

import (
	"flag"
	"fmt"
	"sync"
	"time"
)

type Fork chan struct{}

// pickUp
/*
- Tenta enviar um dado vazio, nesse caso o struct{}{}. Caso o canal realmente esteja vazio
- ele insere e prossegue (pegou o garfo). Se outro filósofo já enviou o dado, o canal ta
- cheio e quem tentar enviar bloqueia até o canal esvaziar com putDown
*/

func pickUp(f Fork) {
	f <- struct{}{}
}

// putDown
/*
- Retira o dado do canal (<-f)
-
- Esvaziando o buffer e liberando o garfo para outro filósofo pegar
*/

func putDown(f Fork) {
	<-f
}

// Estrutura para coletar os dados
type Metrics struct {
	meals     int
	totalWait time.Duration
}

// A Goroutine do Filósofo
/*
- Cada filósofo vai rodar como uma goroutine independente em loop por R rodadas
-
- Ele pensa -> executa o protocolo de pegar os 2 garfos -> come -> devolve os dois garfos
-
- (defer wg.Done()) vai garantir que, ao completar suas R rodadas, o filósofo avisa
- o contador de sincronização que terminou.
*/

func philosopher(
	id int,
	numPhilosophers int,
	rounds int,
	leftFork, rightFork Fork,
	leftIndex, rightIndex int,
	strategy string,
	tableLimit chan struct{},
	wg *sync.WaitGroup,
	metrics *Metrics,
) {
	defer wg.Done()

	for r := 0; r < rounds; r++ {
		// 1. Pensando
		time.Sleep(10 * time.Millisecond)

		// Início da medição de espera pelos recursos
		waitStart := time.Now()

		// 2. Protocolo de aquisição dos garfos
		switch strategy {

		/*
			- Estratégia 1: Deadlock
			- O programa trava rapidamente e exibe um erro de runtime:
			- fatal error: all goroutines are asleep - deadlock!
			-
			- Por que acontece (Teoria de Coffman):
			-
			- Todos os 5 filósofos pegam simultaneamente o garfo da esquerda
			- pickUp(leftFork)
			-
			- O time.Sleep garante que todos terminem de pegaro primeiro garfo antes que
			- qualquer um tente pegar o segundo
			-
			- Agora, cada filósofo está segurando seu garfo esquerdo e tentando pegar o
			- direito com pickUp(rightFork)
			-
			- Porém o garfo direito do filósofo i é justamente o garfo esquerdo do filósfo
			- i + 1, que já está ocupado
			-
			- Fecha-se uma ESPERA CIRCULAR (circular wait): ninguém avança e ninguém solta
			- o garfo que possui (Hold and Wait). O Go detecta que todas as goroutines
			- estão bloqueadas em canais sem nenhum produtor ativo e aborta a execução
		*/

		case "deadlock":
			// Todos pegam esquerda, esperam um instante, tentam direita
			pickUp(leftFork)
			time.Sleep(5 * time.Millisecond) // Força o entrelaçamento para desencadear deadlock
			pickUp(rightFork)

			/*
				- Estratégia 2: Hierarchy
				- O programa executa fluidamente e imprime:
				- "Todos os filósofos concluíram suas refeições com sucesso!"
				-
				- Por que funciona (Teoria de Coffman)
				-
				- Filósofos de 0 a N-2: tem leftIndex < rightIndex, então pegam primeiro a
				- esquerda e depois a direita
				-
				- O último filósofo (N-1): tem garfo esquerdo N-1 e garfo direito 0.
				- Como N-1 > 0, ele INVERTE: tenta pegar primeiro o garfo 0 (menor indice) e
				- só depois o garfo	N-1
				-
				- Ele disputa diretamente o garfo 0 com o Filósofo 0 antes de encostar
				- no garfo N-1
				-
				- Isso destrói a condição de ESPERA CIRCULAR de Coffman: é matemáticamente
				- impossível formar um ciclo direcionado de dependência se todos adquirem
				- recursos em ordem estritamente CRESCENTE DE INDICE
			*/
		case "hierarchy":
			// Quebra de simetria: sempre pega o garfo de menor ID primeiro
			firstFork, secondFork := leftFork, rightFork
			if leftIndex > rightIndex {
				firstFork, secondFork = rightFork, leftFork
			}
			pickUp(firstFork)
			pickUp(secondFork)

			/*
				- Estratégia 3: Conductor
				- Executa sem bloqueios
				-
				- Por que funciona (Teoria de Coffman)
				-
				- Para N = 5, o canal tableLimit tem capacidade 4
				-
				- No máximo 4 filósofos conseguem obter permissão para tentar pegar garfos
				- simultanemente
				-
				- Pelo princípio da Casa dos Pombos, com 5 garfos na mesa e no máximo 4 filósofos
				- tentando comer, PELO MENOS UM FILÓSOFO TERÁ OS DOIS GRAFOS ADJACENTES DISPONÍVEIS,
				- comerá, soltará os garfos e liberará a mesa para o 5 filósofo entrar.
				-
				- Isso elimina a possibilidade de que todos segurem um recurso enquanto esperam
				- pelo proximo (Hold and Wait global)
			*/
		case "conductor":
			// Limitação de presença à mesa: no máximo N-1 filósofos competindo
			tableLimit <- struct{}{}
			pickUp(leftFork)
			pickUp(rightFork)

		default:
			panic("Estratégia desconhecida: " + strategy)
		}

		// Garfos na mão: contabiliza o tempo de espera e a refeição
		metrics.totalWait += time.Since(waitStart)
		metrics.meals++

		// 3. Comendo
		time.Sleep(10 * time.Millisecond)

		// 4. Protocolo de liberação dos garfos
		putDown(leftFork)
		putDown(rightFork)

		if strategy == "conductor" {
			<-tableLimit
		}
	}
}

func main() {
	n := flag.Int("n", 5, "Número de filósofos")
	r := flag.Int("r", 100, "Número de refeições/iterações por filósofo")
	strategy := flag.String("strategy", "hierarchy", "Estratégia: deadlock, hierarchy, conductor")
	flag.Parse()

	if *n < 2 {
		fmt.Println("Erro: O número de filósofos deve ser pelo menos 2.")
		return
	}

	fmt.Printf("Iniciando simulação: N=%d, R=%d, Estratégia=%s\n", *n, *r, *strategy)

	/*
		- Cria um vetor de N garfos
		-
		- A mesa é circular, o que quer dizer que o filósofo i compartilha o garfo esquerdo i
		- e o garfo direito (i + 1) % n
		-
		- Para o último filósofo (i = N - 1), o garfo direito é (N-1 + 1) % N = 0, fechando o
		- o anel da mesa com o filósofo 0.
		-
		- (wg.Wait()) segura o programa principal até que todos os filósofos tenham terminado
		- suas iterações
	*/

	// Inicializa garfos (channels com buffer 1)
	forks := make([]Fork, *n)
	for i := 0; i < *n; i++ {
		forks[i] = make(Fork, 1)
	}

	// Canal para a estratégia do condutor/mesa (capacidade N - 1)
	var tableLimit chan struct{}
	if *strategy == "conductor" {
		tableLimit = make(chan struct{}, *n-1)
	}

	// Vetor de métricas: cada filósofo escreve exclusivamente na sua
	// posição id (evita data race)
	metrics := make([]Metrics, *n)

	var wg sync.WaitGroup
	wg.Add(*n)

	for i := 0; i < *n; i++ {
		leftIndex := i
		rightIndex := (i + 1) % *n

		go philosopher(
			i,
			*n,
			*r,
			forks[leftIndex],
			forks[rightIndex],
			leftIndex,
			rightIndex,
			*strategy,
			tableLimit,
			&wg,
			&metrics[i],
		)
	}

	wg.Wait()
	fmt.Println("Todos os filósofos concluíram suas refeições com sucesso!")

	// Tabela mastigada para a Pessoa 2 colocar direto no relatório/planilha
	fmt.Println("=== DADOS DE EXECUÇÃO (FAIRNESS) ===")
	fmt.Printf("%-12s | %-10s | %-18s\n", "Filósofo ID", "Refeições", "Espera Média")
	fmt.Println("-------------------------------------------------")
	for i := 0; i < *n; i++ {
		avgWait := time.Duration(0)
		if metrics[i].meals > 0 {
			avgWait = metrics[i].totalWait / time.Duration(metrics[i].meals)
		}
		fmt.Printf("Filósofo %-3d | %-10d | %-18v\n", i, metrics[i].meals, avgWait)
	}
}
