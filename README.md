# T1-FFPD

Trabalho em Go: filósofos (`filosofos/`) e produtor/consumidor (`produtor-consumidor/`).
A implementação existente dos filósofos foi preservada. O rascunho anterior de
produtor/consumidor está em `produtor-consumidor/rascunhos/main-original.go.txt`.

## Requisitos e execução

Go 1.22 ou superior. Sem dependências externas. Execute na raiz do projeto:

```sh
go run ./filosofos -strategy hierarchy
go run ./produtor-consumidor -mode channel -k 10 -p 4 -c 4 -n 2500
go run ./produtor-consumidor -mode semaphore -k 10 -p 4 -c 4 -n 2500
go run ./produtor-consumidor -bench -p 4 -c 4 -n 125000
```

`-n` é o número de itens **por produtor**; o total é `p*n`. `-k` deve ser pelo
menos 1; produtores e consumidores devem ser positivos; `-n 0` é permitido.
Use `go run ./produtor-consumidor -h` para consultar as opções.
Também é possível executar `go run produtor-consumidor/main.go`.

- `channel`: channel de itens com capacidade K, fechado somente depois de todos os produtores terminarem. Consumidores drenam o channel e saem ao receber `ok=false`.
- `semaphore`: buffer circular com semáforos de contagem `notEmpty` (inicialmente 0) e `notFull` (inicialmente K), implementados com canais de permissões, e **dois mutexes independentes** — `mutexIn` serializa os produtores no índice de escrita, `mutexOut` serializa os consumidores no índice de leitura. Produtores e consumidores nunca disputam o mesmo lock. Após a produção, insere uma sentinela por consumidor; cada consumidor sai ao retirar sua sentinela. Os semáforos não são fechados. Implementação em `produtor-consumidor/semaforos.go`.
- O consumidor 0 usa `select` com `time.After`. Um timeout é contabilizado e a espera recomeça; não encerra o consumidor nem descarta itens.

## Verificação e race detector

```sh
go test -race ./... -count=1 -timeout=60s
go vet ./...
go run -race ./produtor-consumidor -mode channel -k 1 -p 4 -c 4 -n 2500
go run -race ./produtor-consumidor -mode semaphore -k 1 -p 4 -c 4 -n 2500
# Torna os timeouts visíveis:
go run -race ./produtor-consumidor -mode semaphore -p 1 -c 1 -n 10 -producer-delay 20ms -timeout 2ms
```

O detector exige cgo e um compilador C. Em Windows, LLVM-MinGW ou MSYS2 resolvem;
extraia o zip do LLVM-MinGW com `tar -xf` (segundos) em vez do winget, e exporte
`CGO_ENABLED=1` com o `bin` dele no PATH.
A execução só é bem-sucedida se `produzido == consumido == p*n`, não houver IDs
inválidos/duplicados e o buffer terminar vazio. Falhas retornam código diferente de zero.
Os testes incluem K=1,10,100, múltiplos produtores/consumidores, zero itens,
menos itens que consumidores, parâmetros inválidos e timeouts seguidos de consumo.

## Métricas e experimentos

A saída inclui tempo, throughput (itens consumidos/segundo), contagem por consumidor,
timeouts, ocupação média e pico do buffer. `-json` produz um objeto JSON por execução.
O tempo vai do início da simulação até todos os consumidores terminarem; exclui a
verificação final e a compilação, mas inclui criação das goroutines e encerramento.
A verificação guarda IDs consumidos e usa memória O(p*n), afetando o desempenho.

### Matriz de experimentos

`-bench` executa as duas versões com K = 1, 10 e 100 e imprime a tabela comparativa:

```sh
go run ./produtor-consumidor -bench -p 4 -c 4 -n 125000 -bench-reps 8
go run ./produtor-consumidor -bench -p 4 -c 4 -n 125000 -bench-reps 8 -bench-jsonl produtor-consumidor/resultados.jsonl
```

Throughput e ocupação são medidos em **execuções separadas**: a instrumentação de
ocupação pega um lock e lê o relógio a cada operação, então medir as duas coisas na
mesma execução contaminaria o throughput. Cada combinação roda uma vez de aquecimento
antes de medir, para não pagar alocação e crescimento de heap na primeira amostra.

A carga precisa ser grande o suficiente para a execução durar bem acima da resolução
do relógio. Com 10.000 itens, as combinações de K = 10 e K = 100 terminam em menos de
um tique nesta máquina e o throughput ficaria indefinido; o programa detecta isso e
aborta pedindo `-n` maior, em vez de reportar um número inválido. Com 500.000 itens o
desvio entre repetições fica em 1,5% a 6,5%.

### Ocupação

A ocupação é a **integral do nível do buffer no tempo**, dividida pela duração da
janela — não uma média de amostras. Cada operação informa o nível resultante ao
medidor (`metricas.go`), que acumula `nível × tempo` desde a observação anterior.
A amostragem por ticker foi abandonada porque um intervalo pedido de 100us é entregue
em ~500us no Windows, rendendo menos de 20 amostras por execução.

A janela fecha quando os produtores terminam, antes das sentinelas de encerramento,
para que elas não entrem na média. `-occupancy=false` desliga a medição por completo:
nenhuma operação toca o relógio nem o lock.

Para experimentar velocidades diferentes, acrescente `-consumer-delay 100us` ou
`-producer-delay 100us`, mantendo a mesma configuração nas comparações. Não compare
o desempenho de execuções com e sem `-race`.

Consulte `produtor-consumidor/RELATORIO-PROBLEMA-2.md` para a análise.
