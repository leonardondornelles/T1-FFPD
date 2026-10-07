# Problema 2 — Produtor/Consumidor

## Implementação e invariantes

Duas estratégias compartilham configuração, carga e coleta de métricas. Cada
produtor gera IDs exclusivos `idProdutor*n + índice`. O total esperado é `p*n`.
Contagens e listas são exclusivas de cada goroutine e lidas depois dos WaitGroups.
A verificação final rejeita IDs fora do intervalo, duplicações, quantidade incorreta
e buffer não vazio. Isso é mais forte que apenas comparar contagens, pois uma
duplicação poderia compensar a perda de outro item.

Na versão channel, a capacidade K fornece a limitação e sincronização. O
coordenador aguarda todos os produtores antes de fechar o canal; consumidores
continuam até drenar os itens. Assim, não há envio em canal fechado.

Na versão semáforos (`semaforos.go`), `notFull` inicia com K permissões e
`notEmpty` com zero. Produzir segue: adquirir notFull → `mutexIn` → inserir no
índice `in` → liberar `mutexIn` → sinalizar notEmpty. Consumir segue: adquirir
notEmpty → `mutexOut` → retirar no índice `out` → liberar `mutexOut` → sinalizar
notFull. Os índices avançam módulo K. Durante operações em andamento há permissões
reservadas: não se deve exigir que os comprimentos dos dois canais somem K em todo
instante.

Os **dois mutexes são independentes**, como pede o enunciado ao falar em exclusão
mútua nas posições de escrita *e* de leitura: `mutexIn` serializa os produtores
entre si, `mutexOut` serializa os consumidores entre si, e os dois lados nunca
disputam o mesmo lock. Quem garante que produtor e consumidor não tocam a mesma
posição são os semáforos, não os mutexes — notFull só libera um slot já consumido
e notEmpty só libera um slot já escrito.

Pela mesma razão, a **visibilidade de memória entre os dois lados vem dos
semáforos**. Os mutexes são disjuntos e não estabelecem nenhuma relação entre um
produtor e um consumidor. O que ordena as operações é o canal de permissões: o
envio em notEmpty acontece depois da escrita e antes da recepção correspondente,
e o modelo de memória de Go garante a relação happens-before nesse par. Na direção
inversa, notFull ordena a limpeza do slot antes de ele ser reutilizado.

Daí uma decisão que parece detalhe e não é: o item é copiado **dentro** de
`mutexIn`, e não apenas o índice reservado. Se o produtor apenas reservasse o
índice e escrevesse fora do lock, um produtor que pegou um índice maior poderia
sinalizar notEmpty antes de um produtor com índice menor ter escrito, e o
consumidor leria uma posição ainda vazia. Escrevendo sob o lock, a cadeia
`escrita → unlock → lock → … → send → receive` encadeia também os produtores
entre si.

Os índices dos dois lados ficam separados por um bloco de preenchimento do tamanho
de uma linha de cache. Sem essa separação, `mutexIn`/`in` e `mutexOut`/`out`
cairiam na mesma linha e o falso compartilhamento anularia o ganho de ter dois
mutexes — cada operação de um lado invalidaria a linha no núcleo do outro.

A integridade é verificada por duas vias independentes: contadores `sync/atomic`
mantidos pelas próprias operações do buffer (`integrity()`), e a contagem por IDs
únicos feita em `run`. O programa imprime a asserção `totalProduzido ==
totalConsumido` e falha com código diferente de zero se as duas vias divergirem.

Depois de aguardar os produtores, o coordenador insere uma sentinela por
consumidor. Como não há mais produção e o buffer é FIFO, nenhum item de trabalho
fica atrás das sentinelas. Cada consumidor retira no máximo uma sentinela e sai.
O coordenador aguarda todos, incluindo aqueles ainda processando itens anteriores.
Sentinelas não contam como itens produzidos/consumidos, mas ocupam o buffer.

## Deadlock

Nenhuma goroutine espera uma permissão de semáforo segurando o mutex. Isso permite
que o outro lado altere o buffer e sinalize a permissão necessária. Quando o
buffer está cheio, consumidores podem esvaziá-lo; quando vazio e ainda há produção,
produtores podem preenchê-lo. Com produtores e consumidores positivos, trabalho
finito e goroutines eventualmente escalonadas, os protocolos permitem progresso.
Não há ciclo de espera imposto pelo algoritmo. A sentinela também respeita K,
inclusive K=1; consumidores continuam ativos enquanto o coordenador as insere.

No channel, o fechamento depois do WaitGroup elimina a espera eterna por itens
após a produção. Um consumidor sair ao primeiro timeout quebraria o protocolo:
produtores poderiam ficar bloqueados sem quem drenasse o buffer. Por isso o
consumidor 0 usa `select` + `time.After` apenas para registrar inatividade e tentar
novamente; timeout não prova que a produção terminou.

## Starvation e distribuição

Ausência de deadlock não implica justiça. Não há garantia de divisão uniforme
entre consumidores nem de limite máximo de espera pelo scheduler/mutex/canais.
Consumidores podem receber quantidades muito diferentes, inclusive zero em uma
execução curta. Compare `per_consumer` em várias repetições e cargas. Uma execução
em que todos consumiram não prova ausência de starvation em geral. O término
pressupõe que goroutines prontas eventualmente executem; não há garantia formal
de espera limitada nesta implementação. O consumidor 0 também paga o custo dos
timers, tornando a distribuição assimétrica.

## Metodologia de medição

Três decisões definem o que os números abaixo significam.

**Throughput e ocupação são medidos em execuções separadas.** A instrumentação de
ocupação pega um lock e lê o relógio a cada operação; medir as duas coisas na mesma
execução faria o custo da medição entrar no resultado medido. Nas execuções de
throughput a instrumentação fica desligada e nenhuma operação toca o relógio.

**A ocupação é uma integral no tempo, não uma média de amostras.** Cada operação
informa ao medidor o nível resultante do buffer, que acumula `nível × tempo` desde
a observação anterior; a média é a área dividida pela duração da janela. A primeira
versão usava um ticker de 100us, mas o Windows entrega esse ticker a cada ~500us, o
que rendia menos de 20 amostras por execução — base insuficiente para qualquer
afirmação sobre ocupação. A janela fecha quando os produtores terminam, antes das
sentinelas de encerramento, para que elas não inflem a média.

**A carga precisa ser mensurável.** Com os 10.000 itens sugeridos, as combinações de
K = 10 e K = 100 terminam em menos de um tique do relógio nesta máquina e o
throughput ficaria indefinido. O programa detecta a condição e aborta pedindo `-n`
maior, em vez de publicar um número inválido. A coleta abaixo usa 500.000 itens, com
o que o desvio entre repetições cai para 1,5% a 6,5%.

Cada combinação roda uma vez de aquecimento antes de medir, para que a primeira
amostra não pague alocação e crescimento de heap. O tempo vai do início da simulação
até o último consumidor terminar, incluindo criação de goroutines e encerramento.

## Efeito de K: dados coletados

Execução em 07/10/2026, Go 1.27.0, windows/amd64, AMD Ryzen 5 5600G (12 núcleos
lógicos), sem `-race`. P = 4, C = 4, 500.000 itens por execução, sem atrasos, oito
repetições por combinação. Dados brutos em `resultados.jsonl`. Todas as execuções
verificaram produzido == consumido, sem IDs duplicados e com o buffer vazio ao final.

| Versão | K | Throughput (itens/s) | Desvio | Ocupação média | Pico | Consumidor 0 / média |
|---|---:|---:|---:|---:|---:|---:|
| channel | 1 | 2.546.049 | 3,8% | 0,50 | 1 | 0,71 |
| channel | 10 | 3.848.379 | 2,0% | 5,32 | 10 | 0,56 |
| channel | 100 | 6.244.948 | 6,7% | 54,22 | 100 | 0,44 |
| semaphore | 1 | 1.574.680 | 2,2% | 0,41 | 1 | 0,99 |
| semaphore | 10 | 2.629.038 | 6,0% | 5,64 | 10 | 0,77 |
| semaphore | 100 | 3.910.636 | 5,3% | 63,49 | 100 | 0,74 |

**K desacopla os dois lados, e o efeito é grande.** De K = 1 para K = 100 o
throughput cresce 2,5× nas duas versões. Com K = 1 cada item exige um encontro entre
um produtor e um consumidor: o buffer não absorve nenhuma variação de ritmo e a
ocupação média fica em 0,4–0,5, ou seja, o buffer passa a maior parte do tempo vazio
esperando. Com K = 100 a ocupação média sobe para 54 (channel) e 63 (semáforos), e o
pico encosta em K nas duas versões: o buffer vira de fato um amortecedor, e produtores
deixam de bloquear a cada item.

**O ganho não é proporcional a K.** De K = 1 para K = 10 o throughput cresce ~1,5–1,7×
para um buffer dez vezes maior; de K = 10 para K = 100, ~1,5–1,6× para outro fator de
dez. O gargalo migra da sincronização para o custo por item (escalonamento, cópia,
contenção nos mutexes), que K nenhum elimina.

**O channel é mais rápido que os semáforos em todos os K**, por um fator de 1,5×
(K = 10) a 1,6× (K = 1 e K = 100). É o resultado esperado: o channel é uma primitiva
do runtime, que entrega um item diretamente de um produtor a um consumidor em espera
sem passar pelo buffer, enquanto a versão com semáforos paga, por item, duas operações
de canal de permissões mais um mutex. A vantagem da versão com semáforos não é
desempenho — é tornar o protocolo explícito, com cada passo do algoritmo clássico
visível no código em vez de embutido na semântica da linguagem.

**A ocupação média é sistematicamente maior na versão com semáforos** (63 contra 54
em K = 100). O caminho de consumo é mais caro, os consumidores drenam mais devagar
e a fila fica mais cheia. É a mesma causa do throughput menor, vista pelo outro lado.

**A distribuição entre consumidores não é uniforme, e a causa é identificável.** A
última coluna mostra quanto o consumidor 0 processa em relação à média dos quatro.
O consumidor 0 é o que usa `select` com `time.After`, e aloca um timer novo a cada
iteração do laço, mesmo quando há item disponível. Ele processa 44% a 71% da média no
channel e 74% a 99% nos semáforos. O caso K = 1 com semáforos é o único em que a
distribuição é praticamente perfeita (0,99): o buffer unitário serializa tanto o
acesso que o custo do timer deixa de ser o fator dominante. Quanto maior K, maior o
desequilíbrio — com mais folga, os consumidores sem timer avançam mais rápido.

Esse desequilíbrio é consequência do requisito, não um defeito: o enunciado pede que
pelo menos um consumidor use `select` com `time.After`, e é esse consumidor que paga
a conta. Vale registrar que ele **não é starvation**: o consumidor 0 processa menos,
mas processa continuamente e em volume comparável aos demais.

## Limitações

Os números caracterizam o programa instrumentado nesta máquina, não o custo isolado
das primitivas. A verificação por IDs guarda uma lista por consumidor e usa memória
O(p·n), o que afeta o desempenho das duas versões igualmente. A implementação não
mede latência por item — um buffer maior aumenta o throughput, mas também o tempo que
um item espera na fila, e esse custo não aparece nestas tabelas. Para reproduzir,
mantenha P, C, N, atrasos, versão do Go e ambiente constantes, e não misture medições
com e sem `-race`. A aprovação do detector de corridas cobre os entrelaçamentos que
de fato ocorreram, não é prova de ausência de races.

## Validação

`go vet ./...` limpo e `go test ./... -count=25` sem falhas, cobrindo: K = 1, 10 e
100; zero itens; menos itens que consumidores; parâmetros inválidos; timeouts
seguidos de consumo; ordem FIFO do buffer circular em três voltas completas,
exercitando o wrap-around; invariante de ocupação em [0, K] sob carga concorrente
verificado por uma goroutine vigia; e a ponderação temporal do medidor de ocupação.

**Detector de corridas:** `go test -race ./... -count=15` sem nenhum aviso, e
`go run -race` nas duas versões com K = 1 e K = 100, todas verificando
produzido == consumido. O consumo com timeout foi exercitado isoladamente com
`-mode semaphore -p 1 -c 1 -n 10 -producer-delay 20ms -timeout 2ms`: 72 timeouts
registrados e os 10 itens consumidos, confirmando que o timeout aciona o caminho
alternativo sem encerrar o consumidor nem descartar itens.

Em windows/amd64 o detector exige cgo e um compilador C; esta validação usou
LLVM-MinGW (clang 22.1.8, target x86_64-w64-windows-gnu) com `CGO_ENABLED=1`.
A aprovação do detector cobre os entrelaçamentos que de fato ocorreram nas
execuções, e não constitui prova de ausência de corridas.

A parte de filósofos permaneceu inalterada; o teste do módulo compila esse pacote,
mas não exercita seu protocolo.

## Uso de IA

**Versão com channel, encerramento ordenado e consumo com timeout:** gerada com
assistência do Codex a partir dos requisitos do usuário. A IA inspecionou a
estrutura, preservou a implementação de filósofos e copiou o rascunho anterior para
`rascunhos/main-original.go.txt`; implementou a estratégia, testes e documentação.

**Versão com semáforos, instrumentação e experimentos:** desenvolvida com assistência
do Claude (Claude Code), nas seguintes etapas:

- *Projeto da solução:* discussão do algoritmo clássico com dois mutexes, da origem
  da relação happens-before entre produtor e consumidor e da necessidade de escrever
  o item sob `mutexIn`.
- *Geração de código:* `semaforos.go`, `metricas.go`, `bench.go` e os respectivos
  testes.
- *Depuração:* diagnóstico de dois defeitos de medição — o ticker de 100us entregue
  em ~500us e o throughput `+Inf` em execuções abaixo da resolução do relógio — com
  medições feitas para confirmar cada hipótese antes da correção.
- *Análise dos dados:* execução da matriz de experimentos e interpretação dos
  resultados.
- *Redação:* este documento e as seções correspondentes do README.

Todas as decisões de projeto foram revisadas e são de responsabilidade do autor, que
deve ser capaz de explicar qualquer linha do código e qualquer número das tabelas.

## Referências

- Go Memory Model — https://go.dev/ref/mem — usado para fundamentar a afirmação de
  que o envio em um canal ocorre antes da conclusão da recepção correspondente, que é
  a garantia de visibilidade entre produtor e consumidor nesta implementação.
- Documentação dos pacotes `sync`, `sync/atomic` e `time` — https://pkg.go.dev/sync,
  https://pkg.go.dev/sync/atomic, https://pkg.go.dev/time
- Material da disciplina sobre o problema do buffer limitado com semáforos de
  contagem e exclusão mútua nas posições de escrita e leitura.

> Conferir esta lista antes da entrega: a regra 6 do enunciado exige que **toda**
> fonte efetivamente consultada seja referenciada, e deixar de citar uma é tratado
> como plágio. Acrescente aqui os materiais que cada integrante consultou.
